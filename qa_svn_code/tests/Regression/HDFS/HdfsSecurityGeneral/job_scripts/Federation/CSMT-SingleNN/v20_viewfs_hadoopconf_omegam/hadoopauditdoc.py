import hadoopaudit

fcns  = [ x for x in hadoopaudit.WidgetTestCase.__dict__.keys() if x.startswith('test') ]
print hadoopaudit.WidgetTestCase.__doc__

print "=====tests run:"
for d in fcns:
	print hadoopaudit.WidgetTestCase.__dict__[d].__doc__

