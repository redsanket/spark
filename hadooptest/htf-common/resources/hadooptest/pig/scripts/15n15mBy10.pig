a = load '/HTF/testdata/pig/15n15mBy10.data' using PigStorage(',') as (col1:int,col2:int,col3:int,col4:int,col5:int,col6:int,col7:int,col8:int,col9:int,col10:int,col11:int,col12:int,col13:int,col14:int,col15:int,col16:int,col17:int,col18:int,col19:int,col20:int,col21:int,col22:int,col23:int,col24:int,col25:int,col26:int,col27:int,col28:int,col29:int,col30:int);

cubed_rolled_a = cube a by cube(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15), rollup(col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30);

b = foreach cubed_rolled_a generate flatten(group);

store b into '/HTF/testdata/pig/15n15mBy10.out';
