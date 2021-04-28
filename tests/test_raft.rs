mod proxy;
use async_std::task::{self, block_on, sleep};
use proxy::{ProxyMode, ProxyServer};
use std::{collections::HashSet, time::Duration};
use tempfile::TempDir;
use velli_db::{
    raft::{NodeInfo, RaftNodeHandle, RaftNodeImpl, RaftProposeResult, RaftState},
    Result,
};

type NodeVec = Vec<RaftNodeHandle>;

const BASIC_PORT: u32 = 38000;
const PROXY_PORT_SPAN: u32 = 10000;
const PORT_SPAN_PER_TEST: u32 = 10;
const ONE_LEADER_CHECK_MAX_RETRY_COUNT: u32 = 5;

fn get_leader_list(node_list: &NodeVec) -> Vec<u64> {
    let mut leader_list = vec![];
    for node in node_list {
        let state = block_on(node.state());
        if state == RaftState::Leader {
            leader_list.push(block_on(node.id()));
        }
    }
    leader_list
}

fn check_terms(node_list: &NodeVec) -> bool {
    let first_term = task::block_on(node_list[0].terms());
    for n in node_list[1..].into_iter() {
        if task::block_on(n.terms()) != first_term {
            return false;
        }
    }
    true
}

fn check_one_leader(node_list: &NodeVec) -> Vec<u64> {
    check_one_leader_except(node_list, &HashSet::new())
}

fn id_set(id_list: &Vec<u64>) -> HashSet<u64> {
    id_list.iter().cloned().collect()
}

fn check_one_leader_except(node_list: &NodeVec, except: &HashSet<u64>) -> Vec<u64> {
    let mut leader_list = vec![];
    for count in 0..ONE_LEADER_CHECK_MAX_RETRY_COUNT {
        // wait for the leader to be elected
        wait(Duration::from_secs(1));
        let real_leader_list = get_leader_list(&node_list);
        leader_list = vec![];
        for id in &real_leader_list {
            if !except.contains(id) {
                leader_list.push(id.clone());
            }
        }
        if leader_list.len() != 1 && count == ONE_LEADER_CHECK_MAX_RETRY_COUNT - 1 {
            panic!("Found {} valid leader(s), expect 1.", leader_list.len());
        }
    }
    leader_list
}

fn start_port(test_no: u32) -> u32 {
    BASIC_PORT + test_no * PORT_SPAN_PER_TEST
}

fn proxy_start_port(test_no: u32) -> u32 {
    BASIC_PORT + PROXY_PORT_SPAN + test_no * PORT_SPAN_PER_TEST
}

fn generate_node_info_list(start_port: u32, node_count: u32) -> Vec<NodeInfo> {
    let mut node_info_list = vec![];
    for i in 0..node_count {
        node_info_list.push(NodeInfo::new(
            i as u64,
            format!("0.0.0.0:{}", start_port + i).into(),
        ));
    }
    node_info_list
}

fn prepare_node(node_count: u32, test_no: u32) -> (Vec<RaftNodeHandle>, ProxyServer) {
    let start_port = start_port(test_no);
    let node_info_list = generate_node_info_list(start_port, node_count);
    let mut proxy_port_map = vec![];
    let proxy_start_port = proxy_start_port(test_no);
    for i in 0..node_count {
        proxy_port_map.push((start_port + i, proxy_start_port + i));
    }
    let s = ProxyServer::new(&proxy_port_map);
    let mut node_list = vec![];
    for n in &node_info_list {
        let temp_dir = TempDir::new().expect("unable to create temporary dir.");
        let real_n = NodeInfo::new(
            n.id,
            format!("0.0.0.0:{}", proxy_start_port + n.id as u32).into(),
        );

        let node = RaftNodeImpl::new(
            temp_dir.path().to_path_buf(),
            real_n,
            node_info_list.clone(),
        );
        node_list.push(RaftNodeHandle::new(&node));
        task::spawn(async move {
            node.start().await.unwrap();
        });
    }
    (node_list, s)
}

fn wait(dur: Duration) -> () {
    block_on(sleep(dur));
}

async fn disconnect(server: &mut ProxyServer, node: &RaftNodeHandle, test_no: u32) {
    server.set_proxy_mode(
        start_port(test_no) + node.id().await as u32,
        ProxyMode::Disconnect,
    );
    node.set_node_info_list(
        (0..block_on(node.node_count()))
            .map(|x| NodeInfo {
                id: x as u64,
                address: format!("127.0.0.1:{}", 65000),
            })
            .collect(),
    )
    .await
    .unwrap();
}

async fn connect(server: &mut ProxyServer, node: &RaftNodeHandle, test_no: u32) {
    let start_port = start_port(test_no);
    let node_count = node.node_count().await as u32;
    let node_id = node.id().await;
    server.set_proxy_mode(start_port + node_id as u32, ProxyMode::Normal);
    node.set_node_info_list(generate_node_info_list(start_port, node_count))
        .await
        .unwrap();
}

#[async_std::test]
async fn init_election() -> Result<()> {
    const TEST_NO: u32 = 0;
    let (node_list, _) = prepare_node(3, TEST_NO);

    let leader_list = check_one_leader(&node_list);

    let term = node_list[0].terms().await;

    assert!(check_terms(&node_list));
    assert!(term >= 1);

    // terms and leader should not change after a while
    wait(Duration::from_secs(1));

    assert!(check_terms(&node_list));
    assert_eq!(term, node_list[0].terms().await);
    assert_eq!(leader_list, get_leader_list(&node_list));

    Ok(())
}

#[async_std::test]
async fn re_election() -> Result<()> {
    const TEST_NO: u32 = 1;
    let node_count = 3;
    let (node_list, mut server) = prepare_node(node_count, TEST_NO);

    let leader_list = check_one_leader(&node_list);
    let old_leader = leader_list[0];
    let old_term = node_list[0].terms().await;

    // if the leader disconnects, new leader will be elected
    disconnect(&mut server, &node_list[old_leader as usize], TEST_NO).await;
    wait(Duration::from_secs(1));
    let leader_list = check_one_leader_except(&node_list, &id_set(&vec![old_leader]));
    let new_leader = leader_list[0];
    let new_term = node_list[new_leader as usize].terms().await;
    assert!(new_term > old_term);

    // when the old leader reconnects, new leader should not be distrubed
    connect(&mut server, &node_list[old_leader as usize], TEST_NO).await;
    wait(Duration::from_secs(1));
    let leader_list = check_one_leader(&node_list);
    assert_eq!(new_leader, leader_list[0]);
    // assert_eq!()

    // when there's no quorum, no leader should be elected
    (0..node_count as usize).for_each(|x| {
        if x % 2 == 0 {
            block_on(disconnect(&mut server, &node_list[x], TEST_NO));
        }
    });
    wait(Duration::from_secs(1));
    let leader_list: Vec<u64> = get_leader_list(&node_list)
        .into_iter()
        .filter(|x| x != &new_leader)
        .collect();
    assert_eq!(leader_list.len(), 0);

    // when quorum restores, leader should be elected
    (0..node_count as usize).for_each(|x| {
        if x % 2 == 0 {
            block_on(connect(&mut server, &node_list[x], TEST_NO));
        }
    });
    wait(Duration::from_secs(1));
    check_one_leader(&node_list);

    Ok(())
}

#[macro_use]
extern crate log;
#[async_std::test]
async fn basic_agree() -> Result<()> {
    env_logger::Builder::new()
        .parse_filters("warn,velli_db=info")
        .init();
    const TEST_NO: u32 = 2;
    let (node_list, _) = prepare_node(3, TEST_NO);

    check_one_leader(&node_list);

    let term = node_list[0].terms().await;

    for i in 0..node_list.len() {
        let node = &node_list[i];
        match node.propose(node.id().await.to_string().into_bytes()).await {
            Ok(r) => match r {
                RaftProposeResult::Success(entry) => {
                    info!("Log {}:{} proposed.", entry.term, entry.index);
                    assert_eq!(entry.term, term);
                    assert_eq!(entry.index, i + 1);
                }
                RaftProposeResult::CurrentNoLeader => {
                    panic!("Leader should be elected.");
                }
            },
            Err(e) => panic!("{}", e),
        };
    }
    sleep(Duration::from_secs(5)).await;
    Ok(())
}
