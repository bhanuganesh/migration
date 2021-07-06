create or replace function GetLevel(hid_schema_name varchar(105),
									hid_table_name varchar(105), 
									hid_column_name varchar(105), 
									hid_member ltree)
   returns integer 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_member_level	integer;
	
begin
 	-- logic
	
	EXECUTE format('SELECT coalesce(nlevel(%I),1) FROM %I.%I WHERE %I = $1', hid_column_name, 
				   hid_schema_name, hid_table_name,hid_column_name)
	INTO hid_member_level
	USING hid_member;
	
	RAISE NOTICE 'member level is: % in table: %', hid_member_level, hid_table_name;
		
 	return hid_member_level;
end;
$$