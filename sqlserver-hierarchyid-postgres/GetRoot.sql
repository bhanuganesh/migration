create or replace function GetRoot(hid_schema_name varchar(105),
								   hid_table_name varchar(105), 
								   hid_column_name varchar(105))
   returns ltree 
   language plpgsql
  as
$$
declare
	hid_root	ltree;
begin
 	-- logic
	
		EXECUTE format('select %I from %I.%I where nlevel(%I) = 1 LIMIT 1',
					  hid_column_name,
					  hid_schema_name,
					  hid_table_name,
					  hid_column_name 
					  ) 
		INTO hid_root;
		
		RAISE NOTICE 'Root is: %', hid_root;
			
	return hid_root;
end;
$$