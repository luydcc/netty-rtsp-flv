package com.mediagw.config;

import io.netty.handler.proxy.Socks5ProxyHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;

public class GlobalSocks5ProxyConfig {
    private static final List<Proxy> NO_PROXY=new ArrayList<>();
    private static final Map<String[],List<Proxy>> ALL_PROXY=new HashMap<>();
    private static final Map<String,PasswordAuthentication> PROXY_AUTH=new HashMap<>();
    private static boolean PROXY_ENABLE = false;
    private static final String ALL_MATCH_IP="*";
    public static void addProxy(File proxyFile) {
        if(proxyFile==null||!proxyFile.exists()){
            return;
        }
        Properties props=new Properties();
        try {
            props.load(new FileInputStream(proxyFile));
            props.forEach((k,v)->{
                if(k.toString().endsWith("sock5.proxy.")) {
                    addProxy(v.toString());
                }
            });
        }catch (Exception e) {
            System.out.println("Error loading proxy properties: " + e.getMessage());
        }
    }
    public static void addProxy(String proxyInfo) {
        // user:password@host:ip=xxx;xxx
        String user = null;
        String password = null;
        int index = proxyInfo.lastIndexOf("@");
        if(index>0) {
            int index1=proxyInfo.indexOf(":");
            if(index1>0&&index1<index) {
                user = proxyInfo.substring(0,index1);
                password = proxyInfo.substring(index1+1,index);
            }
        }
        int index2=proxyInfo.lastIndexOf("=");
        if(index2>index) {
            String host = proxyInfo.substring(Math.max(0, index+1),index2);
            String[] proxyIps=proxyInfo.substring(index2+1).split(";");
            addProxy(Proxy.Type.SOCKS, host, user, password, proxyIps);
        }else {
            System.out.println("错误的代理配置: " + proxyInfo);
        }
    }
    public static void addProxy(String host,String user,String password,String ...proxyIp) {
        addProxy(Proxy.Type.SOCKS, host, user, password, proxyIp);
    }
    public static void addProxy(Proxy.Type type,String host,
                                String user,String password,String ...proxyIps) {
        if(!PROXY_ENABLE) {
            PROXY_ENABLE = true;
            NO_PROXY.add(Proxy.NO_PROXY);
            startConfig();
        }
        if(user!=null&&password!=null&&!user.isEmpty()&&!password.isEmpty()) {
            PROXY_AUTH.put(host, new PasswordAuthentication(user, password.toCharArray()));
        }
        String[] arr=host.split(":");
        Proxy proxy=new Proxy(type, new InetSocketAddress(arr[0], Integer.parseInt(arr[1])));
        List<Proxy> proxyList=new ArrayList<>();
        proxyList.add(proxy);
        for(String proxyIp:proxyIps) {
            if(proxyIp.trim().length()<2) {
                continue;
            }
            String[] ipArr=proxyIp.split("\\.");
            if(ipArr.length==4&&ipArr[3].contains("/")) {
                ipArr[3]=ALL_MATCH_IP;
            }
            ALL_PROXY.put(ipArr, proxyList);
        }
    }
    public static Socks5ProxyHandler getSocks5ProxyHandler(String host) {
        String[] arr=host.split("\\.");
        for(Map.Entry<String[],List<Proxy>> entry:ALL_PROXY.entrySet()) {
            String[] checkArr=entry.getKey();
            if(arr.length==checkArr.length) {
                for(int i=0;i<arr.length;i++) {
                    if(arr[i].equals(ALL_MATCH_IP)||arr[i].equals(checkArr[i])) {
                        SocketAddress proxyAddress = entry.getValue().getFirst().address();
                        PasswordAuthentication auth = PROXY_AUTH.get(proxyAddress.toString().substring(1));
                        if(auth!=null){
                            return new Socks5ProxyHandler(proxyAddress, auth.getUserName(), new String(auth.getPassword()));
                        }
                        return new Socks5ProxyHandler(proxyAddress);
                    }
                }
            }
        }
        return null;
    }
    private static void startConfig() {
        System.setProperty("socksProxyVersion", "5");
        ProxySelector.setDefault(new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                String[] arr=uri.getHost().split("\\.");
                for(Map.Entry<String[],List<Proxy>> entry:ALL_PROXY.entrySet()) {
                    String[] checkArr=entry.getKey();
                    if(arr.length==checkArr.length) {
                        for(int i=0;i<arr.length;i++) {
                            if(arr[i].equals(ALL_MATCH_IP)||arr[i].equals(checkArr[i])) {
                                return entry.getValue();
                            }
                        }
                    }
                }
                // ip地址匹配规则，分成4级匹配，如果最后一位有/，则全匹配，通常是/24才是这个匹配，这里简化了
                return NO_PROXY;
            }
            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                System.out.println("connect failed: " + uri);
            }
        });
        // 2. 配置 SOCKS 代理认证（用户名+密码）
        Authenticator.setDefault(new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                String host=getRequestingHost()+":"+getRequestingPort();
                // 替换为真实的代理用户名和密码
                return PROXY_AUTH.get(host);
            }
        });
    }
    static{
        addProxy(new File("proxy.properties"));
    }
}
