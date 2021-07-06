create or replace function GetAncestor(hid_schema_name varchar(105),
									   hid_table_name varchar(105), 
									   hid_column_name varchar(105), 
									   hid_member ltree, 
									   level_number int)
   returns ltree 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_ancestor	ltree;
	hid_member_level	integer;
	
begin
 	-- logic
	
	EXECUTE format('SELECT coalesce(nlevel(%I),1) FROM %I.%I WHERE %I = $1', hid_column_name, 
				   hid_schema_name, hid_table_name,hid_column_name)
	INTO hid_member_level
	USING hid_member;
	
	RAISE NOTICE 'member level is: % in table: %', hid_member_level, hid_table_name;
	
	if(hid_member_level >= 1 and level_number = 0) 
	then
		return hid_member;
	else
		RAISE NOTICE 'In Else block';
		
		EXECUTE format('WITH retrieve AS (	SELECT *, nlevel(%I) lev, row_number() OVER (ORDER BY 
		STRING_TO_ARRAY(%I::text, ''.'')::int[]
		) rownum FROM %I.%I)
		SELECT %I FROM retrieve WHERE rownum < (SELECT max(rownum) FROM retrieve WHERE %I @> $1)
		AND lev = $2
		ORDER BY rownum DESC
		LIMIT 1', 
					   hid_column_name, 
					   hid_column_name,
					   hid_schema_name,
					   hid_table_name,
					   hid_column_name,					   
					   hid_column_name)
		INTO hid_ancestor
		USING hid_member, hid_member_level-level_number;
		
		RAISE NOTICE 'level is: % and ancestor is: %', level_number, hid_ancestor;
			
	end if; 
		
 	return hid_ancestor;
end;
$$