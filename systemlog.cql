drop keyspace logparse;
create keyspace logparse WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

create table logparse.systemlog (
	log_id text,
	log_lineno bigint,
	level text,
	thread text,
	date timestamp,
	source_file text,
	source_lineno bigint,
	message text,
	type text,
	session bigint,
	exception list<text>,
	b_ map<text, boolean>,
	d_ map<text, timestamp>,
	i_ map<text, bigint>,
	f_ map<text, double>,
	s_ map<text, text>,
	primary key (log_id, log_lineno)
) ;
