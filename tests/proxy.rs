use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread::sleep,
    time::Duration,
};

#[derive(Clone, PartialEq)]
pub enum ProxyMode {
    Normal,
    // used in test_proxy
    // but the compiler still complains "variant is never constructed"
    #[allow(dead_code)]
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
