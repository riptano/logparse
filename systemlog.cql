drop keyspace if exists logparse;
create keyspace logparse WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

create table logparse.systemlog (
	id timeuuid primary key,
    log_file text,
    log_line bigint,
    host text,
	level text,
	thread text,
	thread_name text,
	thread_id text,
	date timestamp,
	source_file text,
	source_line bigint,
	message text,
	event_type text,
	exception text,
	b_ map<text, boolean>,
	d_ map<text, timestamp>,
	i_ map<text, bigint>,
	f_ map<text, double>,
	s_ map<text, text>,
) ;
