package hadooptest.automation.utils.http;

public class CustomNameValuePair
{
	private String name;
	private String value;

	public CustomNameValuePair(String name, String value)
	{
		this.name = name;
		this.value = value;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getValue()
	{
		return this.value;
	}

	public void setValue(String value)
	{
		this.value = value;
	}
}
