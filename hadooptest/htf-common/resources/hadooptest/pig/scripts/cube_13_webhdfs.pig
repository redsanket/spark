   a = load 'webhdfs://axonitered-nn1.red.ygrid.yahoo.com/user/patwhite/PigTez/studenttab10k' as (name, age, gpa);
   a_cubed = cube a by cube(name,age,gpa) PARALLEL 100;
   b = foreach a_cubed generate flatten(group);
   store b into '/user/patwhite/PigTez/studenttab10k.out';
