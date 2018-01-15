create table Customer
	(cid int primary key,
	name varchar(50),
	zipcade int,
	password varchar(30)
	UNIQUE(name, password));

create table Reservation
	(rid int primary key,
	cid int references Customer(cid),
	first_flight_fid int references Flights(fid),
	second_flight_fid int references Flights(fid),
	first_carrier_id varchar(5),
	second_carrier_id varchar(5),
	day_of_month int,
	origin_city varchar(20),
	stop varchar(20),
	dest_city varchar(20),
	actual_time float);


insert into customer values (1, 'yiyihao', 98105, 'yiyi');
insert into customer values (2, 'ererhao', 92103, 'erer');
insert into customer values (3, 'sansanhao', 12345, 'sansan');
insert into customer values (4, 'sisihao', 23456, 'sisi');
insert into customer values (5, 'wuwuhao', 34567, 'wuwu');
insert into customer values (6, 'liuliujiang', 45678, 'liuliu');
insert into customer values (7, 'qiqijiang', 56789, 'qiqi');
insert into customer values (8, 'babajiang', 67890, 'baba');
insert into customer values (9, 'jiujiujiang', 24572, 'jiujiu');
insert into customer values (10, 'shishijiang', 48392, 'shishi');

--669687
insert into Reservation values(1, 1, 669687, null, 'AA', null, 17, 'San Diego CA', null, 'Dallas/Fort Worth TX', 169);
insert into Reservation values(2, 1, 669688, null, 'AA', null, 18, 'San Diego CA', null, 'Dallas/Fort Worth TX', 217);
insert into Reservation values(3, 1, 669689, null, 'AA', null, 19, 'San Diego CA', null, 'Dallas/Fort Worth TX', 187);
insert into Reservation values(4, 2, 669690, null, 'AA', null, 20, 'San Diego CA', null, 'Dallas/Fort Worth TX', 193);
insert into Reservation values(5, 2, 669691, null, 'AA', null, 21, 'San Diego CA', null, 'Dallas/Fort Worth TX', 169);
insert into Reservation values(6, 4, 669692, null, 'AA', null, 22, 'San Diego CA', null, 'Dallas/Fort Worth TX', 174);
insert into Reservation values(7, 4, 669693, null, 'AA', null, 23, 'San Diego CA', null, 'Dallas/Fort Worth TX', 177);
insert into Reservation values(8, 7, 669694, null, 'AA', null, 24, 'San Diego CA', null, 'Dallas/Fort Worth TX', 183);
insert into Reservation values(9, 8, 669695, null, 'AA', null, 25, 'San Diego CA', null, 'Dallas/Fort Worth TX', 179);
insert into Reservation values(10,9, 669696, null, 'AA', null, 26, 'San Diego CA', null, 'Dallas/Fort Worth TX', 170);
