package net.floodlightcontroller.initflowtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.openflow.protocol.OFBarrierRequest;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.python.antlr.PythonParser.return_stmt_return;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.IPv4;

public class InitFlowTable  extends TimerTask implements IFloodlightModule, IOFMessageListener {

	protected boolean initSwitchFlage=false;
	protected IFloodlightProviderService floodlightProvider;
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "initflowtable";
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return (type.equals(OFType.FLOW_REMOVED) &&(name.equals("trafficmanage")));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(IOFSwitch sw, OFMessage msg,
			FloodlightContext cntx) {
		// TODO Auto-generated method stub
		if(msg.getType()==OFType.FLOW_REMOVED)
		{
			OFFlowRemoved tempFlowRemoved=new OFFlowRemoved();
			tempFlowRemoved=(OFFlowRemoved)msg;
			if(sw.getId()==1&&tempFlowRemoved.getReason()==OFFlowRemoved.OFFlowRemovedReason.OFPRR_IDLE_TIMEOUT)
			{
				
				 OFMatch match=tempFlowRemoved.getMatch();
				 String keyDst=IPv4.fromIPv4Address(match.getNetworkDestination());
				 String keySrc=IPv4.fromIPv4Address(match.getNetworkSource());
				 OFFlowMod Flowset=new OFFlowMod();
				 OFMatch match1=new OFMatch();
				 String matchStr="ip_dst="+keySrc+","+"ip_src="+keyDst+",dl_type=0x0800";
				 match1.fromString(matchStr);
				 Flowset.setMatch(match1);
				 Flowset.setCommand(OFFlowMod.OFPFC_DELETE);
				 Flowset.setOutPort(OFPort.OFPP_NONE.getValue());
				 IOFSwitch sw1 =floodlightProvider.getSwitches().get((long)5);
				 try{
					   sw1.write(Flowset, null);

			       }catch(IOException e)
					{
						System.out.println(e.getMessage());
					}
			    OFBarrierRequest  req=new OFBarrierRequest();
		        req.setLengthU(OFBarrierRequest.MINIMUM_LENGTH);
		        req.setXid(1);
		        try {
					   sw1.write(req, null);
				       } catch (IOException e1) {
					   e1.printStackTrace();
				    }
			}
		}
		return Command.CONTINUE;
	
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
	    l.add(IFloodlightProviderService.class);
	    return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub

		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		// TODO Auto-generated method stub
		floodlightProvider.addOFMessageListener(OFType.BARRIER_REPLY, this);
		floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
		Timer tempTimer=new Timer();       
		tempTimer.schedule(this, 1000, 2000);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		if(floodlightProvider.getSwitches().size()==5&&!initSwitchFlage)
		{		   
			initSwitchFlage=true;
			System.out.println("initflowtable finish");
			for (IOFSwitch sw : floodlightProvider.getSwitches().values())
			{
				if(2<=sw.getId()&&sw.getId()<=4)setInitFlow1(sw);	
				if(sw.getId()==5)
					{
					   setInitFlow2(sw);
					   
					}
				if(sw.getId()==1)setInitFlow3(sw);
			}
		}
	}
	
	public void setInitFlow1(IOFSwitch sw)
	{

		OFFlowMod Flowset1=new OFFlowMod();
		OFMatch Flowmatch1=new OFMatch();
		String matchStr1="in_port=1";
		Flowmatch1.fromString(matchStr1);
		Flowset1.setMatch(Flowmatch1);
		ArrayList<OFAction>  actions1 = new ArrayList<OFAction>();
		actions1.add(new OFActionOutput((short)2, (short) Short.MAX_VALUE));
		Flowset1.setActions(actions1);
		Flowset1.setBufferId(-1);
		Flowset1.setOutPort(OFPort.OFPP_NONE.getValue());
		Flowset1.setPriority(Short.MAX_VALUE);
		Flowset1.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));
		
		OFFlowMod Flowset2=new OFFlowMod();
		OFMatch Flowmatch2=new OFMatch();
		String matchStr2="in_port=2";
		Flowmatch2.fromString(matchStr2);
		Flowset2.setMatch(Flowmatch2);
		ArrayList<OFAction>  actions2 = new ArrayList<OFAction>();
		actions2.add(new OFActionOutput((short)1, (short) Short.MAX_VALUE));
		Flowset2.setActions(actions2);
		Flowset2.setBufferId(-1);
		Flowset2.setOutPort(OFPort.OFPP_NONE.getValue());
		Flowset2.setPriority(Short.MAX_VALUE);
		Flowset2.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));	
		setflow(sw, Flowset1, Flowset2);
	}
	

	
	public void setInitFlow2(IOFSwitch sw)
	{
        for(int i=3;i<=7;i++)
        {
		OFFlowMod Flowset1=new OFFlowMod();
		OFMatch Flowmatch1=new OFMatch();
		String matchStr1="ip_dst=10.0.0."+i+",dl_type=0x0800";
		Flowmatch1.fromString(matchStr1);
		Flowset1.setMatch(Flowmatch1);
		ArrayList<OFAction>  actions1 = new ArrayList<OFAction>();
		actions1.add(new OFActionOutput((short)i, (short) Short.MAX_VALUE));
		Flowset1.setActions(actions1);
		Flowset1.setBufferId(-1);
		Flowset1.setOutPort(OFPort.OFPP_NONE.getValue());
		Flowset1.setPriority(Short.MAX_VALUE);
		Flowset1.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));
		try{
			   sw.write(Flowset1, null);
	       }catch(IOException e)
			{
				System.out.println(e.getMessage());
			}
	   OFBarrierRequest  req=new OFBarrierRequest();
       req.setLengthU(OFBarrierRequest.MINIMUM_LENGTH);
       req.setXid(1);
       try {
			   sw.write(req, null);
		       } catch (IOException e1) {
			   e1.printStackTrace();
		    }
        }
		
		
	}
	public void setInitFlow3(IOFSwitch sw)
	{

		OFFlowMod Flowset1=new OFFlowMod();
		OFMatch Flowmatch1=new OFMatch();
		String matchStr1="ip_dst=10.0.0.1,dl_type=0x0800";
		Flowmatch1.fromString(matchStr1);
		Flowset1.setMatch(Flowmatch1);
		ArrayList<OFAction>  actions1 = new ArrayList<OFAction>();
		actions1.add(new OFActionOutput((short)1, (short) Short.MAX_VALUE));
		Flowset1.setActions(actions1);
		Flowset1.setBufferId(-1);
		Flowset1.setOutPort(OFPort.OFPP_NONE.getValue());
		Flowset1.setPriority(Short.MAX_VALUE);
		Flowset1.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));
		
		OFFlowMod Flowset2=new OFFlowMod();
		OFMatch Flowmatch2=new OFMatch();
		String matchStr2="ip_dst=10.0.0.2,dl_type=0x0800";
		Flowmatch2.fromString(matchStr2);
		Flowset2.setMatch(Flowmatch2);
		ArrayList<OFAction>  actions2 = new ArrayList<OFAction>();
		actions2.add(new OFActionOutput((short)2, (short) Short.MAX_VALUE));
		Flowset2.setActions(actions2);
		Flowset2.setBufferId(-1);
		Flowset2.setOutPort(OFPort.OFPP_NONE.getValue());
		Flowset2.setPriority(Short.MAX_VALUE);
		Flowset2.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));	
		setflow(sw, Flowset1, Flowset2);
		
		
		
	}
	
	public void setflow(IOFSwitch sw,OFFlowMod Flowset1,OFFlowMod Flowset2)
	{
		try{
			   sw.write(Flowset1, null);
			   sw.write(Flowset2, null);
	       }catch(IOException e)
			{
				System.out.println(e.getMessage());
			}
	    OFBarrierRequest  req=new OFBarrierRequest();
        req.setLengthU(OFBarrierRequest.MINIMUM_LENGTH);
        req.setXid(1);
       try {
			   sw.write(req, null);
		       } catch (IOException e1) {
			   e1.printStackTrace();
		    }
	}

}