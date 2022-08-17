CREATE TABLE IF NOT EXISTS recipes (
	`id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
	`url` varchar(100) NOT NULL,
	`data` text NOT NULL,
	PRIMARY KEY (`id`)
);