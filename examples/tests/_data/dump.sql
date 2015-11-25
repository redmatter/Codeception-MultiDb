-- Server 1 --------------------------------------------------------

CREATE DATABASE Config;

--
-- Table structure for table `Organisation`
--

CREATE TABLE IF NOT EXISTS `Config`.`Organisation` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `Name` varchar(50) NOT NULL,
  `Address` text NOT NULL,
  `Active` enum('YES','NO') NOT NULL,

 PRIMARY KEY (`ID`),
 UNIQUE KEY `Name` (`Name`),
 KEY `Active` (`Active`)
);

--
-- Table structure for table `User`
--

CREATE TABLE IF NOT EXISTS `Config`.`User` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `OrganisationID` int(10) unsigned NOT NULL,
  `Name` varchar(30) NOT NULL,
  `Email` varchar(50) NOT NULL,
  `Address` text NOT NULL,
  `Active` enum('YES','NO') NOT NULL,

 PRIMARY KEY (`ID`),
 KEY `Email` (`Email`,`Active`)
);

--
-- Table structure for table `UserPassword`
--

CREATE TABLE IF NOT EXISTS `Config`.`UserPassword` (
  `UserID` int(10) unsigned NOT NULL,
  `Hash` binary(40) NOT NULL,
  `CreatedAt` datetime NOT NULL,
  `ExpiresAt` datetime NOT NULL,

 KEY `UserID_Hash` (`UserID`,`Hash`),
 KEY `CreatedAt` (`CreatedAt`),
 KEY `ExpiresAt` (`ExpiresAt`),

 CONSTRAINT `UserPassword_User_ID_fk` FOREIGN KEY (`UserID`) REFERENCES `Config`.`User` (`ID`) ON DELETE CASCADE
);

-- Server 2 --------------------------------------------------------

CREATE DATABASE Warehouse;

--
-- Table structure for table `Audit`
--

CREATE TABLE IF NOT EXISTS `Warehouse`.`Audit` (
  `OrganisationID` int(10) unsigned NOT NULL,
  `UserID` int(10) unsigned DEFAULT NULL,
  `JSON` text NOT NULL,

 KEY `UserID` (`UserID`),
 KEY `OrganisationID` (`OrganisationID`)
);
