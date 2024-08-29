# Taotie

Taotie is a simple but powerful data processing tool written in rust.

![TAOTIE ARCHITECTURE](./assets/img/taotie-architecture.png)


## Some useful skills
- [Transfer data from MySQL to ClickHouse and output Parquet](./assets/doc/MySQL2CH.md)

## Practice

### Read nginx log parquet with taotie

```bash
➜  taotie git:(master) taotie
Welcome to Taotie REPL!

taotie〉connect fixtures/nginx_logs.parquet --name nginx               08/29/2024 11:07:00 AM
Connected to dataset: nginx
taotie〉schema nginx                                                   08/29/2024 11:07:03 AM
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| addr        | Utf8      | NO          |
| datetime    | Int64     | NO          |
| method      | Utf8      | NO          |
| url         | Utf8      | NO          |
| protocol    | Utf8      | NO          |
| status      | UInt16    | NO          |
| body_bytes  | UInt64    | YES         |
| referer     | Utf8      | YES         |
| user_agent  | Utf8      | YES         |
+-------------+-----------+-------------+
taotie〉head nginx                                                     08/29/2024 11:07:06 AM
+--------------+------------+--------+----------------------+----------+--------+------------+---------+-----------------------------------------------+
| addr         | datetime   | method | url                  | protocol | status | body_bytes | referer | user_agent                                    |
+--------------+------------+--------+----------------------+----------+--------+------------+---------+-----------------------------------------------+
| 93.180.71.3  | 1431849932 | Get    | /downloads/product_1 | HTTP1_1  | 304    | 0          | -       | Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21) |
| 93.180.71.3  | 1431849923 | Get    | /downloads/product_1 | HTTP1_1  | 304    | 0          | -       | Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21) |
| 80.91.33.133 | 1431849924 | Get    | /downloads/product_1 | HTTP1_1  | 304    | 0          | -       | Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.17) |
| 217.168.17.5 | 1431849934 | Get    | /downloads/product_1 | HTTP1_1  | 200    | 490        | -       | Debian APT-HTTP/1.3 (0.8.10.3)                |
| 217.168.17.5 | 1431849909 | Get    | /downloads/product_2 | HTTP1_1  | 200    | 490        | -       | Debian APT-HTTP/1.3 (0.8.10.3)                |
+--------------+------------+--------+----------------------+----------+--------+------------+---------+-----------------------------------------------+
taotie〉                                                               08/29/2024 11:07:06 AM
```