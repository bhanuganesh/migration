create or replace function GetReparentedValue(hid_schema_name varchar(105),
											  hid_table_name varchar(105), 
											  hid_column_name varchar(105),
											  hid_node ltree,
											  hid_old_root ltree,
											  hid_new_root ltree)
   returns ltree 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_old_parent_level	integer;
	hid_new_root_orgpath	ltree;
begin
 	-- logic
	RAISE NOTICE 'Inside GetReparentedValue Function';
	
	EXECUTE format('SELECT coalesce(nlevel(%I),1) FROM %I.%I WHERE %I = $1', hid_column_name, 
				   hid_schema_name, hid_table_name,hid_column_name)
	INTO hid_old_parent_level
	USING hid_old_root;
	
	RAISE NOTICE 'old parent level is: % in table: %', hid_old_parent_level, hid_old_root;
	
	if(hid_old_root = hid_new_root)
	then
		return hid_old_root;
	else
		
		if(isdescendantof(hid_schema_name, 
						  hid_table_name, 
						  hid_column_name, 
						  hid_old_root, 
						  hid_node))
		then
			RAISE NOTICE 'hid_node is descendant of hid_old_root';
		
			EXECUTE format('WITH retrieve AS ( SELECT *, nlevel(%I) lev, 
							row_number() OVER (ORDER BY STRING_TO_ARRAY(%I::text, ''.'')::int[]) rownum
							FROM %I.%I)
							SELECT $1::ltree || subpath(%I, nlevel(%I)-1) as NewRootOrgPath
							FROM retrieve
							WHERE %I <@ $2
							and lev <> $3
							and %I = $4 LIMIT 1', 
						   hid_column_name,
						   hid_column_name,
						   hid_schema_name,
						   hid_table_name,
						   hid_column_name,
						   hid_column_name,
						   hid_column_name,
						   hid_column_name
						  )
			INTO hid_new_root_orgpath
			USING hid_new_root, hid_old_root, hid_old_parent_level, hid_node;

			return hid_new_root_orgpath;
		else
			return hid_new_root_orgpath;
		end if; 
	end if;	
	return hid_new_root_orgpath;
end;
$$