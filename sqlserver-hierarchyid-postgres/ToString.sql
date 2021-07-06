create or replace function ToString(hid_schema_name varchar(105),
									hid_table_name varchar(105), 
									hid_column_name varchar(105), 
									hid_member ltree)
   returns text 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_member_text		text;
	
begin
 	-- logic
	EXECUTE format('SELECT ltree2text(%I) FROM %I.%I WHERE %I = $1', hid_column_name, 
				   hid_schema_name, hid_table_name,hid_column_name)
	INTO hid_member_text
	USING hid_member;
	
	RAISE NOTICE 'member text is: % in table: %', hid_member_text, hid_table_name;
		
 	return hid_member_text;
end;
$$