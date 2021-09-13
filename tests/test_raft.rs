mod proxy;
use async_std::task::{self, block_on, sleep};
use proxy::{ProxyMode, ProxyServer};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tempfile::TempDir;
use velli_db::{
    raft::{create_raft_node, NodeInfo, RaftNode, RaftNodeHandle, RaftProposeResult, RaftState},
    Result,
};
#[macro_use]
extern crate log;

type NodeMap = HashMap<u64, RaftNodeHandle>;

const BASIC_PORT: u32 = 38000;
const PROXY_PORT_SPAN: u32 = 10000;
const PORT_SPAN_PER_TEST: u32 = 10;
const ONE_LEADER_CHECK_MAX_RETRY_COUNT: u32 = 5;

fn get_leader_list(node_list: &NodeMap) -> Vec<u64> {
    let mut leader_list = vec![];
    for (_, node) in node_list {
        let state = block_on(node.state());
        if state == RaftState::Leader {
            leader_list.push(block_on(node.id()));
        }
    }
    leader_list
}

fn check_terms(node_list: &NodeMap) -> bool {
    let mut term = None;
    for (_, n) in node_list {
        let this_term = task::block_on(n.terms());
        match term {
            Some(t) => {
                if t != this_term {
                    return false;
                }
            }
            None => term = Some(this_term),
        }
    }
    true
}

fn check_one_leader(node_list: &NodeMap) -> Vec<u64> {
    check_one_leader_except(node_list, &HashSet::new())
}

fn id_set(id_list: &Vec<u64>) -> HashSet<u64> {
    id_list.iter().cloned().collect()
}

fn check_one_leader_except(node_list: &NodeMap, except: &HashSet<u64>) -> Vec<u64> {
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

fn generate_node_info_list(start_port: u32, node_count: u64) -> Vec<NodeInfo> {
    let mut node_info_list = vec![];
    for i in 0..node_count {
        node_info_list.push(NodeInfo::new(
            i,
            format!("0.0.0.0:{}", start_port + i as u32).into(),
        ));
    }
    node_info_list
}

fn prepare_node(node_count: u64, test_no: u32) -> (HashMap<u64, RaftNode>, ProxyServer) {
    let start_port = start_port(test_no);
    let node_info_list = generate_node_info_list(start_port, node_count);
    let mut proxy_port_map = vec![];
    let proxy_start_port = proxy_start_port(test_no);
    for i in 0..node_count as u32 {
        proxy_port_map.push((start_port + i, proxy_start_port + i));
    }
    let s = ProxyServer::new(&proxy_port_map);
    let mut node_map = HashMap::new();
    for n in &node_info_list {
        let temp_dir = TempDir::new().expect("unable to create temporary dir.");
        let real_n = NodeInfo::new(
            n.id,
            format!("0.0.0.0:{}", proxy_start_port + n.id as u32).into(),
        );

        let node = create_raft_node(
            temp_dir.path().to_path_buf(),
            real_n,
            node_info_list.clone(),
        )
        .start()
        .unwrap();
        node_map.insert(n.id, node);
        //node_list.push(node);
    }
    (node_map, s)
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
    let node_count = node.node_count().await;
    let node_id = node.id().await;
    server.set_proxy_mode(start_port + node_id as u32, ProxyMode::Normal);
    node.set_node_info_list(generate_node_info_list(start_port, node_count as u64))
        .await
        .unwrap();
}

#[async_std::test]
async fn init_election() -> Result<()> {
    const TEST_NO: u32 = 0;
    const NODE_COUNT: u64 = 3;
    let (node_list, _) = prepare_node(NODE_COUNT, TEST_NO);
    let handle_list = node_list
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(&node)))
        .collect();
    let leader_list = check_one_leader(&handle_list);

    let term = handle_list[&0].terms().await;

    assert!(check_terms(&handle_list));
    assert!(term >= 1);

    // terms and leader should not change after a while
    wait(Duration::from_secs(1));

    assert!(check_terms(&handle_list));
    assert_eq!(term, handle_list[&0].terms().await);
    assert_eq!(leader_list, get_leader_list(&handle_list));

    Ok(())
}

#[async_std::test]
async fn re_election() -> Result<()> {
    const TEST_NO: u32 = 1;
    const NODE_COUNT: u64 = 3;
    let (node_list, mut server) = prepare_node(NODE_COUNT, TEST_NO);
    let handle_list = node_list
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(&node)))
        .collect();
    let leader_list = check_one_leader(&handle_list);
    let old_leader = leader_list[0];
    let old_term = handle_list.values().next().unwrap().terms().await;

    // if the leader disconnects, new leader will be elected
    disconnect(&mut server, &handle_list[&old_leader], TEST_NO).await;
    wait(Duration::from_secs(1));
    let leader_list = check_one_leader_except(&handle_list, &id_set(&vec![old_leader]));
    let new_leader = leader_list[0];
    let new_term = handle_list[&new_leader].terms().await;
    assert!(new_term > old_term);

    // when the old leader reconnects, new leader should not be distrubed
    connect(&mut server, &handle_list[&old_leader], TEST_NO).await;
    wait(Duration::from_secs(1));
    let leader_list = check_one_leader(&handle_list);
    assert_eq!(new_leader, leader_list[0]);

    // when there's no quorum, no leader should be elected
    (0..NODE_COUNT).for_each(|x| {
        if x % 2 == 0 {
            block_on(disconnect(&mut server, &handle_list[&x], TEST_NO));
        }
    });
    wait(Duration::from_secs(1));
    let leader_list: Vec<u64> = get_leader_list(&handle_list)
        .into_iter()
        .filter(|x| x != &new_leader)
        .collect();
    assert_eq!(leader_list.len(), 0);

    // when quorum restores, leader should be elected
    (0..NODE_COUNT).for_each(|x| {
        if x % 2 == 0 {
            block_on(connect(&mut server, &handle_list[&x], TEST_NO));
        }
    });
    wait(Duration::from_secs(1));
    check_one_leader(&handle_list);

    Ok(())
}

#[async_std::test(timeout=Duration::from_secs(1))]
async fn basic_agree() -> Result<()> {
    const TEST_NO: u32 = 2;
    const NODE_COUNT: u64 = 3;
    let (node_list, _) = prepare_node(NODE_COUNT, TEST_NO);
    let handle_list = node_list
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(&node)))
        .collect();
    check_one_leader(&handle_list);

    let term = handle_list.values().next().unwrap().terms().await;

    let mut i = 0;
    for (_, node) in &handle_list {
        match node.propose(format!("Log{}", i).into_bytes()).await {
            Ok(r) => match r {
                RaftProposeResult::Success(entry) => {
                    assert_eq!(entry.term, term);
                    assert_eq!(entry.index as u64, i + 1);
                }
                RaftProposeResult::CurrentNoLeader => {
                    panic!("Leader should be elected.");
                }
            },
            Err(e) => panic!("{}", e),
        };
        i += 1;
    }

    for (_, n) in node_list {
        i = 0;
        for (_, _) in &handle_list {
            let log = n.new_log().await?;
            assert_eq!(log.index as u64, i + 1);
            assert_eq!(
                String::from_utf8(log.content.unwrap()).unwrap(),
                format!("Log{}", i)
            );
            i += 1;
        }
    }
    check_one_leader(&handle_list);
    Ok(())
}

fn random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

async fn check_msg(node: &RaftNode, msg_list: &Vec<Vec<u8>>) -> Result<()> {
    for msg in msg_list {
        let log = node.new_log().await?;
        assert_eq!(&log.content.unwrap(), msg);
    }
    Ok(())
}

async fn check_propose_result(
    node_map: &mut HashMap<u64, RaftNode>,
    propose_id: u64,
    msg_count: usize,
    except_id_set: &HashSet<u64>,
) -> Result<Vec<Vec<u8>>> {
    let mut msg_list = vec![];
    for _ in 0..msg_count {
        msg_list.push(random_string().into_bytes());
    }
    let handle_list: HashMap<u64, RaftNodeHandle> = node_map
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(node)))
        .collect();
    let term = handle_list[&propose_id].terms().await;

    while node_map[&propose_id].has_log() {
        let l = node_map[&propose_id].new_log().await?;
        info!(
            "Node {}: got log with term {} index {}",
            propose_id, l.term, l.index
        );
    }

    for msg in &msg_list {
        match handle_list[&propose_id].propose(msg.clone()).await {
            Ok(r) => match r {
                RaftProposeResult::Success(l) => {
                    info!(
                        "Node {}: proposed log with term {} index {}.",
                        propose_id, l.term, l.index
                    );
                    assert_eq!(l.term, term);
                }
                RaftProposeResult::CurrentNoLeader => {
                    panic!("Leader should be elected.")
                }
            },
            Err(e) => panic!("{}", e),
        }
    }

    check_msg(&node_map[&propose_id], &msg_list).await?;

    let log_list = handle_list[&propose_id].log_history().await;

    for (id, node) in handle_list {
        if except_id_set.contains(&node.id().await) || id == propose_id {
            continue;
        }
        assert!(log_list == node.log_history().await);
    }
    Ok(msg_list)
}

#[async_std::test(timeout=Duration::from_secs(10))]
async fn fail_agree() -> Result<()> {
    const TEST_NO: u32 = 3;
    const NODE_COUNT: u64 = 3;
    let (mut node_map, mut server) = prepare_node(NODE_COUNT, TEST_NO);
    let handle_list = node_map
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(&node)))
        .collect();
    check_one_leader(&handle_list);

    let leader_id = get_leader_list(&handle_list)[0];
    let disconnect_id = (leader_id + 1) % NODE_COUNT;
    info!("Node {} is the leader", leader_id);

    disconnect(&mut server, &handle_list[&disconnect_id], TEST_NO).await;
    info!("Node {} disconnected.", disconnect_id);

    let msg_list = check_propose_result(
        &mut node_map,
        leader_id,
        3,
        &[disconnect_id].iter().cloned().collect(),
    )
    .await?;

    connect(&mut server, &handle_list[&disconnect_id], TEST_NO).await;
    info!("Node {} connected.", disconnect_id);
    sleep(Duration::from_secs(1)).await;
    check_msg(&node_map[&disconnect_id], &msg_list).await?;

    check_propose_result(&mut node_map, leader_id, 3, &HashSet::new()).await?;

    Ok(())
}

#[async_std::test(timeout=Duration::from_secs(10))]
async fn fail_no_agree() -> Result<()> {
    // no agreement if too many followers disconnect
    // use log::LevelFilter;
    // env_logger::builder()
    //     .filter_level(LevelFilter::Error)
    //     .filter_module("velli_db", LevelFilter::Debug)
    //     .filter_module("test_raft", LevelFilter::Info)
    //     .init();
    const TEST_NO: u32 = 4;
    const NODE_COUNT: u64 = 5;
    let (mut node_map, mut server) = prepare_node(NODE_COUNT, TEST_NO);
    let handle_list = node_map
        .iter()
        .map(|(id, node)| (id.clone(), RaftNodeHandle::new(&node)))
        .collect();
    check_one_leader(&handle_list);

    let leader_id = get_leader_list(&handle_list)[0];
    let term = handle_list[&leader_id].terms().await;
    info!("Node {} is the leader", leader_id);

    for i in 0..3 {
        let disconnect_id = (leader_id + i + 1) % NODE_COUNT;
        disconnect(&mut server, &handle_list[&disconnect_id], TEST_NO).await;
        info!("Node {} disconnected.", disconnect_id);
    }

    let mut msg_list = vec![];
    for _ in 0..3 {
        msg_list.push(random_string().into_bytes());
    }

    for msg in &msg_list {
        match handle_list[&leader_id].propose(msg.clone()).await {
            Ok(r) => match r {
                RaftProposeResult::Success(l) => {
                    assert_eq!(l.term, term);
                }
                RaftProposeResult::CurrentNoLeader => {}
            },
            Err(e) => panic!("{}", e),
        }
    }
    sleep(Duration::from_secs(2)).await;
    for (_, node) in &node_map {
        assert_eq!(node.has_log(), false);
    }

    for i in 0..3 {
        let reconnect_id = (leader_id + i + 1) % NODE_COUNT;
        connect(&mut server, &handle_list[&reconnect_id], TEST_NO).await;
        info!("Node {} reconnected.", reconnect_id);
    }
    sleep(Duration::from_secs(2)).await;
    check_one_leader(&handle_list);
    let leader_id = get_leader_list(&handle_list)[0];
    check_propose_result(&mut node_map, leader_id, 3, &HashSet::new()).await?;

    Ok(())
}
