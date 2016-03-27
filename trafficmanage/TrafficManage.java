package net.floodlightcontroller.trafficmanage;



import java.io.IOException;

import java.util.ArrayList;

import java.util.Collection;

import java.util.HashMap;

import java.util.Iterator;

import java.util.Map;



import org.openflow.protocol.OFBarrierRequest;

import org.openflow.protocol.OFFlowMod;

import org.openflow.protocol.OFFlowRemoved;

import org.openflow.protocol.OFFlowRemoved.OFFlowRemovedReason;

import org.openflow.protocol.OFMatch;

import org.openflow.protocol.OFMessage;

import org.openflow.protocol.OFPacketIn;

import org.openflow.protocol.OFPort;

import org.openflow.protocol.OFType;

import org.openflow.protocol.action.OFAction;

import org.openflow.protocol.action.OFActionOutput;

import org.python.antlr.PythonParser.else_clause_return;

import org.python.antlr.PythonParser.return_stmt_return;

import org.python.modules.math;



import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;



import net.floodlightcontroller.core.FloodlightContext;

import net.floodlightcontroller.core.IFloodlightProviderService;

import net.floodlightcontroller.core.IOFMessageListener;

import net.floodlightcontroller.core.IOFSwitch;

import net.floodlightcontroller.core.module.FloodlightModuleContext;

import net.floodlightcontroller.core.module.FloodlightModuleException;

import net.floodlightcontroller.core.module.IFloodlightModule;

import net.floodlightcontroller.core.module.IFloodlightService;

import net.floodlightcontroller.packet.Ethernet;

import net.floodlightcontroller.packet.IPv4;

import sun.print.BackgroundLookupListener;



public class TrafficManage implements IFloodlightModule, IOFMessageListener {



	protected IFloodlightProviderService floodlightProvider;

	protected int highPriorityBW=4;//高优先级链路带宽

	protected int lowPriorityBW=4;//低优先级链路带宽

	protected int shortPathRBW=0;//最短路径链路剩余带宽

	protected int longPathRBW=0;//次短路径链路剩余带宽

	protected short interval=30;//每次流表存活时间

	HashMap<String,OFMatch> HSStorage=new HashMap<String,OFMatch>();//记录分配到最短路径高优先级流信息

	HashMap<String,OFMatch> LSStorage=new HashMap<String,OFMatch>();//记录分配到最短路径低优先级流信息

	HashMap<String,OFMatch> LLStorage=new HashMap<String,OFMatch>();//记录分配到最长路径低优先级流信息

	HashMap<String,Long> LQueue1=new HashMap<String,Long>();//低优先级拥塞队列第一级队列

	HashMap<String,Long> LQueue2=new HashMap<String,Long>();//低优先级拥塞队列第二级队列

	HashMap<String,Long> LQueue3=new HashMap<String,Long>();//低优先级拥塞队列第三级队列

	HashMap<String,Long> HQueue1=new HashMap<String,Long>();//高优先级拥塞队列第一级队列

	HashMap<String,Long> HQueue2=new HashMap<String,Long>();//高优先级拥塞队列第二级队列

	HashMap<String,Long> HQueue3=new HashMap<String,Long>();//高优先级拥塞队列第三级队列

	HashMap<String,Integer> Count=new HashMap<String,Integer>();//拥塞流被调度次数

	@Override

	public String getName() {

		// TODO Auto-generated method stub

		return "trafficmanage";

	}



	@Override

	public boolean isCallbackOrderingPrereq(OFType type, String name) {

		// TODO Auto-generated method stub

		return (type.equals(OFType.PACKET_IN) &&(name.equals("virtualizer")));

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

        Ethernet eth =IFloodlightProviderService.bcStore.get(cntx,IFloodlightProviderService.CONTEXT_PI_PAYLOAD);

        if(msg.getType()==OFType.PACKET_IN)   

		{
			OFPacketIn pi=(OFPacketIn)msg; 

			if(eth.getPayload() instanceof IPv4){

				  IPv4 pkt = (IPv4)eth.getPayload().clone();

				  String dst = IPv4.fromIPv4Address(pkt.getDestinationAddress());

				  String src =IPv4.fromIPv4Address(pkt.getSourceAddress());

				  if(HSStorage.containsKey(src)||LLStorage.containsKey(src)||LSStorage.containsKey(src))return Command.CONTINUE; //忽略已经被分配链路的消息

				  if(Count.containsKey(src))return Command.CONTINUE;//已经在拥塞队列中的流忽略其消息

		          if(dst.equals("10.0.0.1"))

		          {

		        	  if(shortPathRBW>=2)

		        	  {

		        		  processHS(src,dst,interval);//最短路径上有可利用带宽

        	        	  
		        	  }

		        	  else if(shortPathRBW<2&&!LSStorage.isEmpty())

		        	  {


		        		  processHL(src,dst,interval);//最短路径上没有可利用带宽，有低优先级流占有了最短路径

		        		 

		        	  }else

		        	  {

		        		      long time=System.currentTimeMillis();

		        		      HQueue1.put(src,(Long)time);//拥塞加入高优先级拥塞队列

		        			  Count.put(src,(Integer)0);

		        	  }

		        	 

		          }

		          if(dst.equals("10.0.0.2"))

		          {

		        	  if(shortPathRBW>=2)

		        	  {

		        		  processLS(src,dst,interval);//最短路径有可利用带宽

		        	
		        	  }

		        	  else if(shortPathRBW<2&&longPathRBW>=2)

		        	  {

		        		  processLL(src,dst,interval);//最短路径没有可利用带宽，最长路径有可利用带宽

		        		

		        	  }else

		        	  {

		        		  long time=System.currentTimeMillis();

	        		      LQueue1.put(src,(Long)time);//没有可利用带宽加入拥塞队列

	        			  Count.put(src,(Integer)0);

		        	  }

		        	  

		          }

		          

			}

		   

	   }

       if(msg.getType()==OFType.FLOW_REMOVED&&sw.getId()==1)

       {
             /*当有流离开时对流进行处理*/
    	    OFFlowRemoved tempFlowRemoved=new OFFlowRemoved();

			tempFlowRemoved=(OFFlowRemoved)msg;

			if(tempFlowRemoved.getReason()!=OFFlowRemoved.OFFlowRemovedReason.OFPRR_DELETE)

			{

			    OFMatch match=tempFlowRemoved.getMatch();

			    String keyDst=IPv4.fromIPv4Address(match.getNetworkDestination());

			    if(tempFlowRemoved.getReason()==OFFlowRemoved.OFFlowRemovedReason.OFPRR_IDLE_TIMEOUT)

			    {

			       if(Count.containsKey(keyDst))

			       {

			    	    Count.remove(keyDst);//拥塞的流已经完成传输


			       }

			       if(Count.isEmpty()||(HQueue1.isEmpty()&&HQueue2.isEmpty()&&HQueue3.isEmpty()&&LQueue1.isEmpty()&&LQueue2.isEmpty()&&LQueue3.isEmpty()))

			       {

			            processIdletime1(sw, keyDst);//有流退出没有拥塞时处理

			       }else

				   {

				    	processIdletime2(sw, keyDst);//有流退出拥塞时处理

				   }

			    }

			    else

			    {

			    	processHardTime(sw,keyDst);//当流运行时间已到强制其退出

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

		shortPathRBW=highPriorityBW;

		longPathRBW=highPriorityBW;

	}



	@Override

	public void startUp(FloodlightModuleContext context) {

		// TODO Auto-generated method stub

		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);

        floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);

	}

	
    /*为高优先级流分配最短路径处理函数*/
	public void processHS(String src,String dst,short interval)

	{

		shortPathRBW=shortPathRBW-2;

		OFMatch match=new OFMatch();

		match=addflowtable(src,dst,(short)4,interval);

		HSStorage.put(src,match);

	}
     /*为高优先级流分配最短路径并且将低优先级流调离处理函数*/
	public void processHL(String src,String dst,short interval )

	{

		OFFlowMod Flowset=new OFFlowMod();

		OFMatch match=new OFMatch();

		for(OFMatch tempMatch:LSStorage.values())

		{

			match=tempMatch;

		}
        
		if(longPathRBW<2)

		{
             /*当调离低优先级流时，次短路径上没有可利用带宽直接删除其流表*/

			Flowset.setCommand(OFFlowMod.OFPFC_DELETE);

			Flowset.setMatch(match);

			Flowset.setOutPort(OFPort.OFPP_NONE.getValue());

			IOFSwitch sw =floodlightProvider.getSwitches().get((long)1);

			setflow(sw, Flowset);

			String matchStr="ip_dst="+"10.0.0.2,"+"ip_src="+IPv4.fromIPv4Address(match.getNetworkDestination())+",dl_type=0x0800";

			OFMatch match1=new OFMatch();

			match1.fromString(matchStr);

			Flowset.setMatch(match1);

			Flowset.setOutPort(OFPort.OFPP_NONE.getValue());

			sw =floodlightProvider.getSwitches().get((long)5);

			setflow(sw, Flowset);

			

		}else

		{

		    /*当调离低优先级流时，次短路径上有可利用带宽时为其下发流表 让其走次短路径*/
			longPathRBW=longPathRBW-2;

			OFMatch match1=new OFMatch();

			match1=addflowtable(IPv4.fromIPv4Address(match.getNetworkDestination()),IPv4.fromIPv4Address(match.getNetworkSource()),(short)3,interval);

			LLStorage.put(IPv4.fromIPv4Address(match.getNetworkDestination()),match1);

			

		}

		LSStorage.remove(IPv4.fromIPv4Address(match.getNetworkDestination()));	

		OFMatch match2=new OFMatch();

		match2=addflowtable(src,dst,(short)4,interval);

		HSStorage.put(src,match2);

	}

	
    /*低优先级流最短路径处理函数*/
	public void processLS(String src,String dst,short interval)

	{

		shortPathRBW=shortPathRBW-2;

		OFMatch match=new OFMatch();

		match=addflowtable(src,dst,(short)4,interval);

		LSStorage.put(src,match);

	}
    /*低优先级流此短路径处理函数*/
	public void processLL(String src,String dst,short interval)

	{

		longPathRBW=longPathRBW-2;

		OFMatch match=new OFMatch();

		match=addflowtable(src,dst,(short)3,interval);

		LLStorage.put(src,match);

	}
    /*配置流表函数*/
	public OFMatch addflowtable(String matchStr1,String matchStr2,short outPut,short interval)

	{

		OFFlowMod Flowset2=new OFFlowMod();

		OFMatch Flowmatch2=new OFMatch();

		String matchStr="ip_src="+matchStr2+",ip_dst="+matchStr1+",dl_type=0x0800";

		Flowmatch2.fromString(matchStr);

		Flowset2.setMatch(Flowmatch2);

		ArrayList<OFAction>  actions2 = new ArrayList<OFAction>();

		actions2.add(new OFActionOutput((short)(outPut), (short) Short.MAX_VALUE));

		Flowset2.setActions(actions2);

		Flowset2.setBufferId(-1);

		Flowset2.setOutPort(OFPort.OFPP_NONE.getValue());

		Flowset2.setPriority(Short.MAX_VALUE);

		Flowset2.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));

		Flowset2.setIdleTimeout((short)5);

		Flowset2.setHardTimeout(interval);

		Flowset2.setFlags(OFFlowMod.OFPFF_SEND_FLOW_REM);

		IOFSwitch sw =floodlightProvider.getSwitches().get((long)1);

		setflow(sw, Flowset2);


		OFFlowMod Flowset1=new OFFlowMod();

		OFMatch Flowmatch1=new OFMatch();

		matchStr="ip_dst="+matchStr2+",ip_src="+matchStr1+",dl_type=0x0800";

		Flowmatch1.fromString(matchStr);

		Flowset1.setMatch(Flowmatch1);

		ArrayList<OFAction>  actions1 = new ArrayList<OFAction>();

		actions1.add(new OFActionOutput((short)(outPut-2), (short) Short.MAX_VALUE));

		Flowset1.setActions(actions1);

		Flowset1.setBufferId(-1);

		Flowset1.setOutPort(OFPort.OFPP_NONE.getValue());

		Flowset1.setPriority(Short.MAX_VALUE);

		Flowset1.setLength((short)(OFFlowMod.MINIMUM_LENGTH+8));

		Flowset1.setHardTimeout(interval);

		Flowset1.setFlags(OFFlowMod.OFPFF_SEND_FLOW_REM);

		sw =floodlightProvider.getSwitches().get((long)5);

		setflow(sw, Flowset1);
		return Flowmatch2;

	}

	
    /*下发流表函数*/
	public void setflow(IOFSwitch sw,OFFlowMod Flowset)

	{

		try{

			   sw.write(Flowset, null);


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

	

	/*处理有流退出没有拥塞队列时处理函数*/
	public void processIdletime1(IOFSwitch sw,String keyDst)

	{

		if(LLStorage.containsKey(keyDst))

	    {

		     LLStorage.remove(keyDst);

             longPathRBW=longPathRBW+2;


             

	    }

	    else if(LSStorage.containsKey(keyDst))

	    {

	    	 LSStorage.remove(keyDst);

	    	 shortPathRBW=shortPathRBW+2;

		     if(!LLStorage.isEmpty())

		     {

		    	OFFlowMod Flowset=new OFFlowMod();

		 		OFMatch match1=new OFMatch();

		 		for(OFMatch tempMatch:LLStorage.values())

		 		{

		 			match1=tempMatch;

		 		}		 		

		 		longPathRBW=longPathRBW+2;

		 		LLStorage.remove(IPv4.fromIPv4Address(match1.getNetworkDestination()));

		 		processLS(IPv4.fromIPv4Address(match1.getNetworkSource()),IPv4.fromIPv4Address(match1.getNetworkDestination()),interval);

		 		Flowset.setCommand(OFFlowMod.OFPFC_DELETE);

				Flowset.setMatch(match1);

				System.out.println(Flowset);

				setflow(sw, Flowset);

		     }

	    }

	    else if(HSStorage.containsKey(keyDst))

	    {

	    	 HSStorage.remove(keyDst);

	    	 shortPathRBW=shortPathRBW+2;

	    	 if(!LLStorage.isEmpty())

	    	 {

	    		    OFFlowMod Flowset=new OFFlowMod();

			 		OFMatch match1=new OFMatch();

			 		for(OFMatch tempMatch:LLStorage.values())

			 		{

			 			match1=tempMatch;

			 		}		 		

			 		longPathRBW=longPathRBW+2;

			 		LLStorage.remove(IPv4.fromIPv4Address(match1.getNetworkDestination()));

			 		processLS(IPv4.fromIPv4Address(match1.getNetworkDestination()),IPv4.fromIPv4Address(match1.getNetworkSource()),interval);

			 		Flowset.setCommand(OFFlowMod.OFPFC_DELETE);

					Flowset.setMatch(match1);

					System.out.println(Flowset);

					setflow(sw, Flowset);

	    	 }

	    	 

	    }

	}
    /*处理有流退出有拥塞队列时处理函数*/
	public void processIdletime2(IOFSwitch sw,String keyDst)

	{

		if(HSStorage.containsKey(keyDst)||LSStorage.containsKey(keyDst))

		{

			if(LSStorage.containsKey(keyDst))

			{

				LSStorage.remove(keyDst);

			}else

			{

			    HSStorage.remove(keyDst);

			}

			shortPathRBW=shortPathRBW+2;

			processQueue1(sw, keyDst);

		}

		if(LLStorage.containsKey(keyDst))

		{

			longPathRBW=longPathRBW+2;

			LLStorage.remove(keyDst);

			processQueue2(sw, keyDst);

		}

	}
	
    /*最短路径上流退出执行函数*/
	public void processQueue1(IOFSwitch sw,String keyDst)

	{

		if(!HQueue1.isEmpty())

		{

			String src=getHQueueStr(HQueue1);

			Count.put(src, (Integer)1);

			processHS(src, "10.0.0.1", interval);

			HQueue1.remove(src);

		}

		else if(!HQueue2.isEmpty())

		{

			String src=getHQueueStr(HQueue2);

			Count.put(src, (Integer)2);

			processHS(src, "10.0.0.1", (short)(2*interval));

			HQueue2.remove(src);

		}

		else if(!HQueue3.isEmpty())

		{

			String src=getHQueueStr(HQueue3);

			Count.put(src, (Integer)3);

			processHS(src, "10.0.0.1",(short)(3*interval));

			HQueue3.remove(src);

		}else if(!LQueue1.isEmpty())

		{

			String src=getHQueueStr(LQueue1);

			Count.put(src, (Integer)1);

			processLS(src, "10.0.0.2",interval);

			LQueue1.remove(src);

		}else if(!LQueue2.isEmpty())

		{

			String src=getHQueueStr(LQueue2);

			Count.put(src, (Integer)2);

			processLS(src, "10.0.0.2",(short)(2*interval));

			LQueue2.remove(src);

		}

		else if(!LQueue3.isEmpty())

		{

			String src=getHQueueStr(LQueue3);

			Count.put(src, (Integer)3);

			processLS(src, "10.0.0.2",(short)(3*interval));

			LQueue3.remove(src);

		}

	

}
    /*次短路径上流退出执行函数*/
	public void processQueue2(IOFSwitch sw,String keyDst)

	{

		 if(!LQueue1.isEmpty())

			{

				String src=getHQueueStr(LQueue1);

				Count.put(src, (Integer)1);

				processLL(src, "10.0.0.2",interval);

				LQueue1.remove(src);

			}else if(!LQueue2.isEmpty())

			{

				String src=getHQueueStr(LQueue2);

				Count.put(src, (Integer)2);

				processLL(src, "10.0.0.2",(short)(2*interval));

				LQueue2.remove(src);

			}

			else if(!LQueue3.isEmpty())

			{

				String src=getHQueueStr(LQueue3);

				Count.put(src, (Integer)3);

				processLL(src, "10.0.0.2",(short)(3*interval));

				LQueue3.remove(src);

			}

	}
	
   /*流被强制退出时执行函数*/
	public void processHardTime(IOFSwitch sw,String keyDst)

	{



		if(Count.isEmpty())

		{

			if(HSStorage.containsKey(keyDst))

			{

				shortPathRBW=shortPathRBW+2;

				processHS(keyDst, "10.0.0.1", interval);

			}

			if(LSStorage.containsKey(keyDst))

			{   

				shortPathRBW=shortPathRBW+2;

				processLS(keyDst, "10.0.0.2", interval);

			}

			if(LLStorage.containsKey(keyDst))

			{   longPathRBW=longPathRBW+2;

				processLL(keyDst, "10.0.0.2", interval);

			}



			return;

		}else if(LLStorage.containsKey(keyDst)&&judgeQueue())

		{


			longPathRBW=longPathRBW+2;

			processLL(keyDst, "10.0.0.2", interval);

		}else 

		{


			if(!Count.containsKey(keyDst))

			{


				Count.put(keyDst, (Integer)1);

				if(HSStorage.containsKey(keyDst))HQueue2.put(keyDst, (Long)System.currentTimeMillis());

				else

				{


					LQueue2.put(keyDst, (Long)System.currentTimeMillis());	

				}

			}

			if(HSStorage.containsKey(keyDst))

			{

				if(Count.get(keyDst)==1)HQueue2.put(keyDst, (Long)System.currentTimeMillis());

				if(Count.get(keyDst)==2)HQueue3.put(keyDst, (Long)System.currentTimeMillis());

				if(Count.get(keyDst)==3)Count.remove(keyDst);

			}else

			{

				if(Count.get(keyDst)==1)LQueue2.put(keyDst, (Long)System.currentTimeMillis());

				if(Count.get(keyDst)==2)LQueue3.put(keyDst, (Long)System.currentTimeMillis());

				if(Count.get(keyDst)==3)Count.remove(keyDst);

			}



				if(HSStorage.containsKey(keyDst)||LSStorage.containsKey(keyDst))

				{


					if(LSStorage.containsKey(keyDst))

					{

						LSStorage.remove(keyDst);

					}else

					{

					    HSStorage.remove(keyDst);

					}

					shortPathRBW=shortPathRBW+2;

					processQueue1(sw, keyDst);

				}

				if(LLStorage.containsKey(keyDst))

				{
					

					longPathRBW=longPathRBW+2;

					LLStorage.remove(keyDst);

					processQueue2(sw, keyDst);

				}

		}

		

	}



    public String getHQueueStr(HashMap<String,Long> Queue)

    {

    	int i=0;

    	long miniTime=0;

    	String miniStr="";

    	Iterator iter = Queue.keySet().iterator();

		while (iter.hasNext()) {

			  Object key = iter.next();

			if(i==0)

			{

		      miniTime=Queue.get(key).longValue();

		      miniStr=(String)key;

		      i++;

			}else 

			{

				if(miniTime>Queue.get(key).longValue())

				{

					miniTime=Queue.get(key).longValue();

				    miniStr=(String)key;

				}

			}

		}

		return miniStr;

    }

   public boolean judgeQueue()

   {

	   if((!HQueue1.isEmpty()||!HQueue2.isEmpty()||!HQueue3.isEmpty()))return true;

	   else 

	   {

		   return false;

	   }

   }

  

   
}

