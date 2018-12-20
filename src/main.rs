#![feature(attr_literals)]
#![feature(custom_attribute)]
extern crate account_eventsourcing;
#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
extern crate chrono;
#[macro_use]
extern crate eventsourcing;
#[macro_use]
extern crate eventsourcing_derive;
extern crate json;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate uuid;

use cdrs::{types::prelude::*};
use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::types::rows::Row;
use chrono::{DateTime, Utc};
use eventsourcing::{eventstore::MemoryEventStore, prelude::*, Result};
use serde_json::Error;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, Event)]
#[event_type_version("1.0")]
#[event_source("")]
enum BankEvent {
    FundsWithdrawn(AccountDetail, EventDetail),
    FundsDeposited(AccountDetail, EventDetail),
}

enum BankCommand {
    WithdrawFunds(AccountDetail, EventDetail),
    DepositFunds(AccountDetail, EventDetail),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AccountDetail {
    acctnum: String,
    balance: u32,
    trans_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct EventDetail {
    generation: u64,
    timestamp: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AccountData {
    account: AccountDetail,
    payload: EventDetail,

}

impl AggregateState for AccountData {
    fn generation(&self) -> u64 {
        self.payload.generation
    }
}

struct Account;

impl Aggregate for Account {
    type Event = BankEvent;
    type State = AccountData;
    type Command = BankCommand;

    fn apply_event(state: &Self::State, evt: Self::Event) -> Result<Self::State> {
        let now: DateTime<Utc> = Utc::now();
        let state: AccountData = match evt {
            BankEvent::FundsWithdrawn(adetail, payload) => AccountData {
                account: (AccountDetail {
                    acctnum: state.account.acctnum.to_owned(),
                    balance: state.account.balance - adetail.balance,
                    trans_type: "Withdraw".to_string(),
                }),
                payload: (EventDetail {
                    generation: state.payload.generation + 1,
                    timestamp: now.to_string(),
                }),
            },
            BankEvent::FundsDeposited(adetail, payload) => AccountData {
                account: (AccountDetail {
                    acctnum: state.account.acctnum.to_owned(),
                    balance: state.account.balance + adetail.balance,
                    trans_type: "Deposit".to_string(),
                }),
                payload: (EventDetail {
                    generation: state.payload.generation + 1,
                    timestamp: now.to_string(),
                }),
            },
        };
        Ok(state)
    }

    fn handle_command(_state: &Self::State, cmd: Self::Command) -> Result<Vec<Self::Event>> {
        // SHOULD DO: validate state and command

        // if validation passes...
        let evts:Vec<BankEvent> = match cmd {
            BankCommand::DepositFunds(adetail, payload) => vec![BankEvent::FundsDeposited(
                AccountDetail {
                    acctnum: adetail.acctnum,
                    trans_type: "Deposit".to_string(),
                    balance: adetail.balance,
                }, EventDetail {
                    generation: payload.generation,
                    timestamp: payload.timestamp,
                })],
            BankCommand::WithdrawFunds(adetail, payload) => vec![BankEvent::FundsWithdrawn(
                AccountDetail {
                    acctnum: adetail.acctnum,
                    trans_type: "withdraw".to_string(),
                    balance: adetail.balance,
                }, EventDetail {
                    generation: 0,
                    timestamp: payload.timestamp,
                })],
        };
        Ok(evts)
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct FullPayload {
    account_event_detail: AccountDetail,
    event_detail: EventDetail,
}

fn insert_event(initial_state: &AccountData, event: &BankEvent, connection: Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>) -> String {


    let insert_struct_cql: &str = "INSERT INTO account.events \
                           (account_id, event, event_id) VALUES (?, ?, ?)";


    let full_payload: FullPayload = match event {
        BankEvent::FundsDeposited(adetail, payload) | BankEvent::FundsWithdrawn(adetail, payload) => FullPayload {
            account_event_detail: adetail.to_owned(),
            event_detail: payload.to_owned(),
        }
    };

    let payload_json: String = serde_json::to_string(&full_payload).unwrap();

    let uuid: Uuid = Uuid::new_v4();

    connection
        .query_with_values(insert_struct_cql,
                           query_values!(initial_state.account.acctnum.clone(),
                           payload_json,uuid.to_string()))
        .expect("insert error ");

    "Event Inserted into Database".to_string()
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq, Serialize, Deserialize)]
struct StoredEvent {
    account_id: String,
    event_id: String,
    event: String,
}

fn get_final_state(acct_num: String, connection: Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>) -> AccountData {

    let select_struct_cql: &str = "Select * from account.events where account_id = ?";

    let event_rows: Vec<Row> = connection
        .query_with_values(select_struct_cql, query_values!(acct_num.clone()))
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into row");

    let mut final_state: AccountData = AccountData {
        account: (AccountDetail {
            acctnum: acct_num,
            balance: 0,
            trans_type: String::new(),
        }),
        payload: (EventDetail { generation: 1, timestamp: String::new() }),
    };


    let mut getevent: StoredEvent = StoredEvent {
        account_id: String::new(),
        event_id: String::new(),
        event: String::new(),
    };


    for event in event_rows {
        getevent = StoredEvent::try_from_row(event).expect("into GetEvent Struct");
        let bankevent: FullPayload = serde_json::from_str(&getevent.event).unwrap();

        if (bankevent.account_event_detail.trans_type == "Deposit") {
            let deposit = BankCommand::DepositFunds(bankevent.account_event_detail,
                                                    bankevent.event_detail);
            let post_depo = Account::handle_command(&final_state, deposit)
                .unwrap();
            final_state = Account::apply_event(&final_state, post_depo[0].clone()).unwrap();
        } else {
            let withdraw = BankCommand::WithdrawFunds(bankevent.account_event_detail,
                                                      bankevent.event_detail);
            let post_withdraw = Account::handle_command(&final_state,
                                                        withdraw).unwrap();
            final_state = Account::apply_event(&final_state, post_withdraw[0].clone())
                .unwrap();
        }
    }
    final_state
}

fn main() {
    let now: DateTime<Utc> = Utc::now();

    let account_store: MemoryEventStore = MemoryEventStore::new();

    let connection: Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>> = account_eventsourcing::dbconnection::connection();

    let deposit: BankCommand = BankCommand::DepositFunds(AccountDetail {
        acctnum: "SAVINGS100".to_string(),
        balance: 500,
        trans_type: "".to_string(),
    }, EventDetail {
        generation: 1,
        timestamp: now.to_string(),
    });

    let initial_state: AccountData = AccountData {
        account: AccountDetail {
            acctnum: "SAVINGS100".to_string(),
            balance: 0,
            trans_type: "".to_string(),
        },
        payload: EventDetail {
            generation: 1, timestamp: now.to_string()
        },
    };


    let mut deposit_events: Vec<BankEvent> = Account::handle_command(&initial_state, deposit)
        .unwrap();

    let state: AccountData= Account::apply_event(&initial_state, deposit_events[0].clone())
        .unwrap();

    println!("{}", insert_event(&initial_state, &deposit_events[0],connection));

    deposit_events.pop();


    let withdraw = BankCommand::WithdrawFunds(AccountDetail {
        acctnum: "SAVINGS100".to_string(),
        balance: 400,
        trans_type: "".to_string(),
    }, EventDetail {
        generation: 1,
        timestamp: now.to_string(),
    });

    deposit_events.push(Account::handle_command(&state, withdraw).unwrap().pop()
        .unwrap());

    println!("{}", insert_event(&state, &deposit_events[0], connection));


    let state2: AccountData = Account::apply_event(&state, deposit_events[0].clone()).unwrap();

    // code for getting events from database

    let final_state: AccountData = get_final_state("SAVINGS100".to_string(),connection);
    println!("{:#?}", state2);
    println!("{:#?}", final_state);
}