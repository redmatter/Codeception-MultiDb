CREATE DATABASE DemoWarehouse;
CREATE USER 'demo'@'%' IDENTIFIED BY 'notsecure';
GRANT ALL PRIVILEGES ON `DemoWarehouse`.* TO 'demo'@'%';
