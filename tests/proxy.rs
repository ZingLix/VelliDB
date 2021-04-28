use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread::{sleep, JoinHandle},
    time::Duration,
};

#[derive(Clone, PartialEq)]
pub enum ProxyMode {
    Normal,
    Delay(Duration),
    Disconnect,
}

pub struct ProxyServer {
    proxy_map: HashMap<u32, Proxy>,
}

impl ProxyServer {
    pub fn new(port_map: &Vec<(u32, u32)>) -> Self {
        let mut proxy_map = HashMap::new();
        for (from, to) in port_map {
            let server = Proxy::new(from.clone(), to.clone());
            proxy_map.insert(from.clone(), server);
        }
        Self { proxy_map }
    }

    pub fn set_proxy_mode(&mut self, port: u32, proxy_mode: ProxyMode) {
        self.proxy_map
            .get_mut(&port)
            .unwrap()
            .set_proxy_mode(proxy_mode);
    }
}

struct Proxy {
    proxy_mode: Arc<RwLock<ProxyMode>>,
}

impl Proxy {
    pub fn new(from: u32, to: u32) -> Self {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", from)).unwrap();
        let proxy_mode = Arc::new(RwLock::new(ProxyMode::Normal));
        let proxy_m = proxy_mode.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mode = Arc::clone(&proxy_m);
                if *mode.read().unwrap() == ProxyMode::Disconnect {
                    stream.shutdown(Shutdown::Both).unwrap();
                }
                std::thread::spawn(move || {
                    let mut buffer = [0 as u8; 1024];
                    let conn = TcpStream::connect(format!("0.0.0.0:{}", to));
                    let mut conn = match conn {
                        Ok(c) => c,
                        Err(_) => {
                            stream.shutdown(Shutdown::Both).unwrap();
                            return;
                        }
                    };
                    let mut conn_clone = conn.try_clone().unwrap();
                    let mut stream_clone = stream.try_clone().unwrap();
                    std::thread::spawn(move || {
                        let mut buffer = [0 as u8; 1024];
                        loop {
                            let n = conn_clone.read(&mut buffer).unwrap();
                            if n == 0 {
                                stream_clone.shutdown(Shutdown::Both).unwrap();
                                break;
                            }
                            match stream_clone.write(&buffer[0..n]) {
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                    });
                    loop {
                        let n = match stream.read(&mut buffer) {
                            Ok(n) => {
                                if n == 0 {
                                    break;
                                } else {
                                    n
                                }
                            }
                            Err(_) => break,
                        };

                        let m;
                        {
                            m = mode.read().unwrap().clone();
                        }
                        match m {
                            ProxyMode::Normal => {}
                            ProxyMode::Delay(d) => sleep(d),
                            ProxyMode::Disconnect => match stream.shutdown(Shutdown::Both) {
                                Ok(()) => {}
                                Err(_) => break,
                            },
                        }
                        match conn.write(&buffer[0..n]) {
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                });
            }
        });
        Self { proxy_mode }
    }

    pub fn set_proxy_mode(&mut self, proxy_mode: ProxyMode) {
        let mut g = self.proxy_mode.write().unwrap();
        *g = proxy_mode;
    }
}

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
    handle
}

#[test]
fn proxy_basic() {
    env_logger::Builder::new()
        .parse_filters("warn,velli_db=info")
        .init();
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
            conn.read(&mut buf).unwrap();
            let elapsed = start.elapsed();
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
