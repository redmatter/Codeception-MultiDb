CREATE DATABASE DemoConfig;
CREATE USER 'demo'@'%' IDENTIFIED BY 'notsecure';
GRANT ALL PRIVILEGES ON `DemoConfig`.* TO 'demo'@'%';
