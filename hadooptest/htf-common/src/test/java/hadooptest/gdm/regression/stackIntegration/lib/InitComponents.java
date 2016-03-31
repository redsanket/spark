package hadooptest.gdm.regression.stackIntegration.lib;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;

public class InitComponents implements java.util.concurrent.Callable<Boolean>{

	private String componentHostName;
	private String scriptPath;
	private String componentName;
	private CommonFunctions commonFunctions;
	private StackComponent stackComponent;

	public InitComponents( ) {}
	
	public InitComponents(String componentName, String componentHostName , String scriptPath) {
		this.setComponentName(componentName);
		this.setComponentHostName(componentHostName);
		this.setScriptPath(scriptPath);
		this.commonFunctions = new CommonFunctions();
	}
	
	public InitComponents(StackComponent stackComponent , String scriptPath) {
		this.stackComponent = stackComponent;
		this.setComponentName(this.stackComponent.getStackComponentName() );
		this.setScriptPath(scriptPath);
		this.setComponentHostName(this.stackComponent.getHostName());
		TestSession.logger.info("component name = " + this.getComponentName()  + "   script Location - " + this.getScriptPath()  + "   hostName - " + this.getComponentHostName());
		this.commonFunctions = new CommonFunctions();
	}
	
	public String getComponentHostName() {
		return componentHostName;
	}

	public void setComponentHostName(String componentHostName) {
		this.componentHostName = componentHostName;
	}

	public String getScriptPath() {
		return scriptPath;
	}

	public void setScriptPath(String scriptPath) {
		this.scriptPath = scriptPath;
	}
	
	public String getComponentName() {
		return componentName;
	}

	public void setComponentName(String componentName) {
		this.componentName = componentName;
	}

	@Override
	public Boolean call() throws Exception {
		boolean flag = false;
		if (this.commonFunctions.isJarFileExist(this.getComponentHostName()) == true) {
			flag = this.commonFunctions.copyTestCases(this.getComponentName() , this.getScriptPath() , this.getComponentHostName());
			this.stackComponent.setScriptCopied(flag);
			this.stackComponent.setScriptLocation(this.commonFunctions.getScriptLocation());
		}
		return flag;
	}
}
