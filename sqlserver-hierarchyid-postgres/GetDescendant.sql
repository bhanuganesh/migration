create or replace function GetDescendant(hid_schema_name varchar(105),
									   	 hid_table_name varchar(105), 
									     hid_column_name varchar(105),
										 hid_parent ltree,
									     hid_child_1 ltree,
										 hid_child_2 ltree)
   returns ltree 
   language plpgsql
  as
$$
declare 
	-- variable declaration
	hid_descendant	ltree;
	hid_child_1_level	integer;
	hid_child_2_level	integer;

begin
 	
	-- logic
	if(hid_parent is null)
	then
		return hid_parent;	
			
	elsif (hid_parent is not null and hid_child_1 is null and hid_child_2 is null)
	then
		
		-- create child for parent
		EXECUTE format('SELECT %I || $1::ltree AS concatenated FROM %I.%I WHERE %I = $2 LIMIT 1 ', 
					   hid_column_name,
					   hid_schema_name,
					   hid_table_name,
					   hid_column_name
					  )
		INTO hid_descendant
		USING '1', hid_parent;
		
		RAISE NOTICE 'New Descendant is: % for parent: %', hid_descendant, hid_parent;

		return hid_descendant;
		
	elsif (hid_parent is not null and hid_child_1 is not null and hid_child_2 is null)
	then
		
		RAISE NOTICE 'inside hid_child_1 is not null and hid_child_2 is null';
		
		-- create a child of parent greater than hid_child_1
		if(isdescendantof(hid_schema_name, 
						  hid_table_name, 
						  hid_column_name, 
						  hid_parent, 
						  hid_child_1))
		then
			RAISE NOTICE 'hid_child_1 is descendant of hid_parent';
			
			if(hid_parent = hid_child_1)
			then
				return hid_descendant;
			else
			
				EXECUTE format('SELECT nlevel(%I) FROM %I.%I WHERE %I = $1 LIMIT 1 ', 
							   hid_column_name,
							   hid_schema_name,
							   hid_table_name,
							   hid_column_name
							  )
				INTO hid_child_1_level
				USING hid_child_1;
				
				RAISE NOTICE 'child 1 level: %', hid_child_1_level;
				
				EXECUTE format('WITH retrieve AS ( SELECT *, nlevel(%I) lev, 
							    row_number() OVER (ORDER BY STRING_TO_ARRAY(%I::text, ''.'')::int[]) rownum
								FROM %I.%I)
								SELECT $1::ltree || (ltree2text(subpath(%I, nlevel(%I)-1))::int + 1)::text as NewDescendant
				  				FROM retrieve
								WHERE rownum = (SELECT max(rownum) FROM retrieve WHERE %I <@ $1 and lev = $2)', 
							   hid_column_name,
							   hid_column_name,
							   hid_schema_name,
							   hid_table_name,
							   hid_column_name,
							   hid_column_name,
							   hid_column_name
							  )
				INTO hid_descendant
				USING hid_parent, hid_child_1_level;
				
				RAISE NOTICE 'New descendant: %', hid_descendant;
				
				return hid_descendant;
								
			end if; 
			
			return hid_descendant;
		else
		
			return hid_descendant;
		
		end if; 
		
	elsif (hid_parent is not null and hid_child_2 is not null and hid_child_1 is null)
	then
		
		RAISE NOTICE 'inside hid_child_2 is not null and hid_child_1 is null';
		
		-- create a child of parent less than hid_child_2
		if(isdescendantof(hid_schema_name, 
						  hid_table_name, 
						  hid_column_name, 
						  hid_parent, 
						  hid_child_2))
		then
			RAISE NOTICE 'hid_child_2 is descendant of hid_parent';
			
			if(hid_parent = hid_child_2)
			then
				return hid_descendant;
			else
			
				EXECUTE format('SELECT nlevel(%I) FROM %I.%I WHERE %I = $1 LIMIT 1 ', 
							   hid_column_name,
							   hid_schema_name,
							   hid_table_name,
							   hid_column_name
							  )
				INTO hid_child_2_level
				USING hid_child_2;
				
				RAISE NOTICE 'child 2 level: %', hid_child_2_level;
				
				EXECUTE format('WITH retrieve AS ( SELECT *, nlevel(%I) lev, 
							    row_number() OVER (ORDER BY STRING_TO_ARRAY(%I::text, ''.'')::int[]) rownum
								FROM %I.%I)
								SELECT $1::ltree || (ltree2text(subpath(%I, nlevel(%I)-1))::int - 1)::text as NewDescendant
				  				FROM retrieve
								WHERE rownum = (SELECT max(rownum) FROM retrieve WHERE %I <@ $2 and lev = $3)', 
							   hid_column_name,
							   hid_column_name,
							   hid_schema_name,
							   hid_table_name,
							   hid_column_name,
							   hid_column_name,
							   hid_column_name
							  )
				INTO hid_descendant
				USING hid_parent, hid_child_2, hid_child_2_level;
				
				RAISE NOTICE 'New descendant: %', hid_descendant;
				
				return hid_descendant;
								
			end if; 
			
			return hid_descendant;
		else
		
			return hid_descendant;
		
		end if; 
		
	elsif (hid_parent is not null and hid_child_1 is not null and hid_child_2 is not null)
	then
	
		if not (isdescendantof(hid_schema_name, 
						  hid_table_name, 
						  hid_column_name, 
						  hid_parent, 
						  hid_child_1))
		then
			return hid_descendant;
		end if;
		
		if not (isdescendantof(hid_schema_name, 
						  hid_table_name, 
						  hid_column_name, 
						  hid_parent, 
						  hid_child_2))
		then
			return hid_descendant;
		end if;
		
		if(hid_parent = hid_child_1 or hid_parent = hid_child_2 or hid_child_1 = hid_child_2)
		then
			return hid_descendant;
		else

			EXECUTE format('SELECT nlevel(%I) FROM %I.%I WHERE %I = $1 LIMIT 1 ', 
						   hid_column_name,
						   hid_schema_name,
						   hid_table_name,
						   hid_column_name
						  )
			INTO hid_child_1_level
			USING hid_child_1;

			RAISE NOTICE 'child 1 level: %', hid_child_1_level;

			EXECUTE format('WITH retrieve AS ( SELECT *, nlevel(%I) lev, 
							row_number() OVER (ORDER BY STRING_TO_ARRAY(%I::text, ''.'')::int[]) rownum
							FROM %I.%I)
							SELECT $1::ltree || (ltree2text(subpath(%I, nlevel(%I)-1))::int + 1)::text as NewDescendant
							FROM retrieve
							WHERE rownum = (SELECT max(rownum) FROM retrieve WHERE %I <@ $2 and lev = $3)
						   and (ltree2text(subpath($4, nlevel(%I)-1))::int -
							ltree2text(subpath($2, nlevel(%I)-1))::int) > 1 ', 
						   hid_column_name,
						   hid_column_name,
						   hid_schema_name,
						   hid_table_name,
						   hid_column_name,
						   hid_column_name,
						   hid_column_name,
						   hid_column_name,
						   hid_column_name
						  )
			INTO hid_descendant
			USING hid_parent, hid_child_1, hid_child_1_level, hid_child_2;

			RAISE NOTICE 'New descendant: %', hid_descendant;

			return hid_descendant;

		end if; 
	
	end if;
	
	return hid_descendant;
	
end;
$$