#![feature(attr_literals)]
#![feature(custom_attribute)]
#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate eventsourcing;
#[macro_use]
extern crate eventsourcing_derive;
extern crate account_eventsourcing;
extern crate json;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate uuid;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::types::rows::Row;
use eventsourcing::{eventstore::MemoryEventStore, prelude::*, Result};
use serde_json::Error;
use uuid::Uuid;
use cdrs::{types::prelude::*};

#[derive(Serialize, Deserialize, Debug, Clone, Event)]
#[event_type_version("1.0")]
#[event_source("")]
enum BankEvent {
    FundsWithdrawn(AccountDetail),
    FundsDeposited(AccountDetail),
}

enum BankCommand {
    WithdrawFunds(AccountDetail),
    DepositFunds(AccountDetail),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AccountDetail {
    acctnum: String,
    balance: u32,
    trans_type: String,
}


#[derive(Clone, Debug,  PartialEq,Serialize, Deserialize)]
struct AccountData {
    account: AccountDetail,
    generation: u64,
}

impl AggregateState for AccountData {
    fn generation(&self) -> u64 {
        self.generation
    }
}

struct Account;

impl Aggregate for Account {
    type Event = BankEvent;
    type State = AccountData;
    type Command = BankCommand;

    fn apply_event(state: &Self::State, evt: Self::Event) -> Result<Self::State> {
        let state = match evt {
            BankEvent::FundsWithdrawn(adetail) => AccountData {
                account: (AccountDetail { acctnum: state.account.acctnum.to_owned(), balance: state.account.balance - adetail.balance, trans_type: "Withdraw".to_string() }),
                generation: state.generation + 1,
            },
            BankEvent::FundsDeposited(adetail) => AccountData {
                account: (AccountDetail { acctnum: state.account.acctnum.to_owned(), balance: state.account.balance + adetail.balance, trans_type: "Deposit".to_string() }),
                generation: state.generation + 1,
            },
        };
        Ok(state)
    }

    fn handle_command(_state: &Self::State, cmd: Self::Command) -> Result<Vec<Self::Event>> {
        // SHOULD DO: validate state and command

        // if validation passes...
        let evts = match cmd {
            BankCommand::DepositFunds(adetail) => vec![BankEvent::FundsDeposited(AccountDetail { acctnum: adetail.acctnum, trans_type: "Deposit".to_string(), balance: adetail.balance })],
            BankCommand::WithdrawFunds(adetail) => vec![BankEvent::FundsWithdrawn(AccountDetail { acctnum: adetail.acctnum, trans_type: "withdraw".to_string(), balance: adetail.balance })],
        };
        Ok(evts)
    }
}


fn insert_event(initial_state: &AccountData, event: &BankEvent) -> String {
    let connection = account_eventsourcing::dbconnection::connection();

    let insert_struct_cql = "INSERT INTO account.events \
                           (account_id, event, event_id) VALUES (?, ?, ?)";

    let acc_detail = match event {
        BankEvent::FundsDeposited(adetail) => adetail,
        BankEvent::FundsWithdrawn(adetail) => adetail,
    };

    let account_json = serde_json::to_string(&acc_detail).unwrap();
let z=Uuid::new_v4();

    connection
        .query_with_values(insert_struct_cql, query_values!(initial_state.account.acctnum.clone(),account_json,z.to_string()))
        .expect("insert error ");

    "Event Inserted into Database".to_string()
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq, Serialize, Deserialize)]
struct StoredEvent {
    account_id: String,
    event_id: String,
    event: String,
}

fn get_final_state(acct_num: String) -> AccountData {
    let connection = account_eventsourcing::dbconnection::connection();

    let select_struct_cql = "Select * from account.events where account_id = ?";

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
            balance: 800,
            trans_type: String::new(),
        }),
        generation: 1,
    };

    let mut getevent: StoredEvent = StoredEvent{
        account_id: String::new(),
        event_id: String::new(),
        event: String::new()
    };

    for event in event_rows {
       getevent = StoredEvent::try_from_row(event).expect("into GetEvent Struct");
        let bankevent: AccountDetail = serde_json::from_str(&getevent.event).unwrap();

        if (bankevent.trans_type == "Deposit") {
            let deposit = BankCommand::DepositFunds(bankevent);
            let post_depo = Account::handle_command(&final_state, deposit).unwrap();
            final_state = Account::apply_event(&final_state, post_depo[0].clone()).unwrap();
        } else {
            let withdraw = BankCommand::WithdrawFunds(bankevent);
            let post_withdraw = Account::handle_command(&final_state, withdraw).unwrap();
            final_state = Account::apply_event(&final_state, post_withdraw[0].clone()).unwrap();
        }
    }
    final_state
}

fn main() {
    let account_store = MemoryEventStore::new();

    let deposit = BankCommand::DepositFunds(AccountDetail { acctnum: "SAVINGS100".to_string(), balance: 500, trans_type: "".to_string() });

    let initial_state = AccountData {
        account: (AccountDetail { acctnum: "SAVINGS100".to_string(), balance: 800, trans_type: "".to_string() }),
        generation: 1,
    };


    let mut post_deposit = Account::handle_command(&initial_state, deposit).unwrap();

    let state = Account::apply_event(&initial_state, post_deposit[0].clone()).unwrap();

    println!("{}", insert_event(&initial_state,&post_deposit[0]));

    post_deposit.pop();


    let withdraw = BankCommand::WithdrawFunds(AccountDetail { acctnum: "SAVINGS100".to_string(), balance: 400, trans_type: "".to_string() });

    post_deposit.push(Account::handle_command(&state, withdraw).unwrap().pop().unwrap());

    println!("{}", insert_event(&state,&post_deposit[0]));


    let state2 = Account::apply_event(&state, post_deposit[0].clone()).unwrap();


    // code for getting events from database

    let final_state = get_final_state("SAVINGS100".to_string());
    println!("{:#?}", state2);
    println!("{:#?}", final_state);
}