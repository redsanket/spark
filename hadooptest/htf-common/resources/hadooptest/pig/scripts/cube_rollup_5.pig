
a = load '$protocol://$namenode/user/patwhite/PigTez/cube_rollup_5.data' using PigStorage('') as (col1:chararray,col2:chararray,col3:int,col4:int,col5:float,col6:float,col7:int,col8:int,col9:chararray,col10:int,col11:chararray,col12:chararray,col13:int,col14:int,col15:float,col16:float,col17:int,col18:int,col19:chararray,col20:int);
cubed_rolled_a = cube a by cube(col1,col2,col3,col4), rollup(col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20);
b = foreach cubed_rolled_a generate flatten(group), SUM(cube.col20) as total20;
store b into '/user/patwhite/PigTez/cube_rollup_5.out';
