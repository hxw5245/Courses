ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
use trips;
SELECT `trips_by_distance`.`Level`, `trips_by_distance`.`Date`, `trips_by_distance`.`State_Postal_Code`, `trips_by_distance`.`County_Name`, `trips_by_distance`.`Number_of_Trips`, `trips_by_distance`.`Week`, `trips_by_distance`.`Month`
FROM `trips_by_distance`;




