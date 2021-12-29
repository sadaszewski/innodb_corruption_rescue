# innodb_corruption_rescue
Save InnoDB after corruption caused by power loss, hardware failure, etc.

1. Run safedump.py
2. Run saferestore.py

This works by dumping whatever it can (row by row) and giving the MySQL/MariaDB server the chance to restart (on the fly) after eventual crashes caused by InnoDB corruption. Independently you might need to set innodb_force_recovery to 1 or higher.
