create or replace function IsDescendantOf(hid_schema_name varchar(105),
									   	  hid_table_name varchar(105), 
									      hid_column_name varchar(105), 
									      hid_parent ltree, 
									      hid_child	ltree)
   returns boolean 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_descendant	integer;
	
begin
 	-- logic
			EXECUTE format('WITH retrieve AS 
					   	  (SELECT %I FROM %I.%I WHERE %I <@ $1 )
						   SELECT count(1) FROM retrieve WHERE %I = $2', 
					   hid_column_name, 
					   hid_schema_name,
					   hid_table_name,
					   hid_column_name,					   
					   hid_column_name)
		INTO hid_descendant
		USING hid_parent, hid_child;
		
		RAISE NOTICE 'child is: % of parent %: %', hid_child, hid_parent, hid_descendant;
	
		if( hid_descendant = 0 )
		then
			return false;
		else
			return true;
		end if;	
end;
$$
