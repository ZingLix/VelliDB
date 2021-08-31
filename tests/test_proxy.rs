mod proxy;

use proxy::{ProxyMode, ProxyServer};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{sleep, JoinHandle},
    time::Duration,
};

fn create_echo_server(start_port: u32, range: u32) -> Vec<JoinHandle<()>> {
    let mut handle = vec![];
    for p in start_port..start_port + range {
        handle.push(std::thread::spawn(move || {
            let listener = TcpListener::bind(format!("0.0.0.0:{}", p)).unwrap();

            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        std::thread::spawn(move || loop {
                            let mut read = [0; 1024];
                            match stream.read(&mut read) {
                                Ok(n) => {
                                    if n == 0 {
                                        break;
                                    }
                                    stream.write(&read[0..n]).unwrap();
                                    stream.write(p.to_string().as_bytes()).unwrap();
                                }
                                Err(_) => {
                                    panic!();
                                }
                            }
                        });
                    }
                    Err(_) => {
                        panic!();
                    }
                }
            }
        }));
    }
    // wait for all servers to start listening
    sleep(Duration::from_millis(50));
    handle
}

#[test]
fn proxy_basic() {
    let mut port_map = vec![];
    let start_port = 48800;
    let range = 5;
    let port_span = 10000;
    for p in start_port..start_port + range {
        port_map.push((p, p + port_span));
    }
    ProxyServer::new(&port_map);
    create_echo_server(start_port + port_span, range);
    for _ in 0..5 {
        for p in start_port..start_port + range {
            let mut conn = TcpStream::connect(format!("0.0.0.0:{}", p)).unwrap();
            let mut buf = [0; 64];
            let test_str = vec![
                format!("sent to: {}, received from: ", p),
                format!("another try! {}", p),
            ];

            for str in test_str {
                conn.write(str.as_bytes()).unwrap();
                sleep(Duration::from_millis(50));
                let n = conn.read(&mut buf).unwrap();
                let recv = String::from_utf8(buf[0..n].to_vec()).unwrap();
                assert_eq!(recv, format!("{}{}", str, p + port_span));
            }
        }
    }
}

#[test]
fn proxy_delay() {
    let mut port_map = vec![];
    let start_port = 48810;
    let range = 2;
    let port_span = 10000;
    for p in start_port..start_port + range {
        port_map.push((p, p + port_span));
    }
    let mut s = ProxyServer::new(&port_map);
    create_echo_server(start_port + port_span, range);

    let delay_time = vec![10, 100];

    let mut conn = TcpStream::connect(format!("0.0.0.0:{}", start_port)).unwrap();
    let mut buf = [0; 64];
    let test_str = vec![
        format!("sent to: {}, received from: ", start_port),
        format!("another try! {}", start_port),
    ];

    for d in delay_time {
        s.set_proxy_mode(
            start_port,
            ProxyMode::Delay(Duration::from_millis(d.clone())),
        );

        for str in &test_str {
            conn.write(str.as_bytes()).unwrap();
            let start = std::time::Instant::now();
            let n = conn.read(&mut buf).unwrap();
            let elapsed = start.elapsed();
            assert!(n != 0);
            assert!(elapsed.as_millis() >= d as u128);

            // wait and recv all residual data
            sleep(Duration::from_millis(100));
            conn.set_read_timeout(Some(Duration::from_millis(20)))
                .unwrap();
            conn.read(&mut buf).ok();
            conn.set_read_timeout(None).unwrap();
        }
    }
}

#[test]
fn proxy_disconnect() {
    let mut port_map = vec![];
    let start_port = 48820;
    let range = 2;
    let port_span = 10000;
    for p in start_port..start_port + range {
        port_map.push((p, p + port_span));
    }
    let mut s = ProxyServer::new(&port_map);
    create_echo_server(start_port + port_span, range);

    let mut conn = TcpStream::connect(format!("0.0.0.0:{}", start_port)).unwrap();
    let mut buf = [0; 64];
    let test_str = format!("sent to: {}, received from: ", start_port);

    conn.write(test_str.as_bytes()).unwrap();
    sleep(Duration::from_millis(50));
    let n = conn.read(&mut buf).unwrap();
    assert_ne!(n, 0);
    assert_eq!(
        String::from_utf8(buf[0..n].to_vec()).unwrap(),
        format!("{}{}", test_str, start_port + port_span)
    );

    // wait and recv all residual data
    sleep(Duration::from_millis(50));
    conn.set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    conn.read(&mut buf).ok();
    conn.set_read_timeout(None).unwrap();

    s.set_proxy_mode(start_port, ProxyMode::Disconnect);

    conn.write(test_str.as_bytes()).unwrap();
    let n = conn.read(&mut buf).unwrap();
    assert_eq!(n, 0);

    let mut conn = TcpStream::connect(format!("0.0.0.0:{}", start_port)).unwrap();
    assert_eq!(conn.read(&mut buf).unwrap(), 0);
}
