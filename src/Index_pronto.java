

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import br.com.magoserver.exception.MagoIndexCubeException;
import br.com.magoserver.exception.MagoKernelException;
import br.com.magoserver.exception.MagoSerializeException;
import br.com.magoserver.kernel.util.DriverConfig;
import br.com.magoserver.kernel.util.SerializeObjectImpl;
import br.com.magoserver.kernel.util.TMStop;
import br.com.magoserver.model.Cube;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public final class Index_pronto {
	
	public static void execute(TMStop stop, Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, IntCollection>> oneDCuboids, Int2ObjectOpenHashMap<String[]> baseCuboid, Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap> measureValues, Cube cube, Set<String>inputs) throws MagoIndexCubeException, MagoSerializeException, MagoKernelException, IOException, ParseException{
		String[] attributes=null;
		int numD=0;
		int numM=0;
		String text = null;		
		
		long time = System.nanoTime();
		String[] filesF = null;
		File tMAGOinputDIR = new File(cube.getFullFileName());
		filesF = tMAGOinputDIR.list();
		tMAGOinputDIR=null;
		
		LinkedList<String> files = new LinkedList<String>();
		
		for(String aux:filesF)
			if(!inputs.contains(aux)){
				inputs.add(aux);
				files.add(aux);
			}
					
		//criando as estruturas com nomes da base do user
		Properties propertiesD = new Properties();
		Properties propertiesDtype = new Properties();
		
		String path = DriverConfig.getPah();
	    propertiesD.load(new FileInputStream(path+"/conf/catalogs/"+cube.getCatalogName()+"/"+cube.getName()+"/dimensionsnames.mg"));
	   
	    numD= propertiesD.size();
	    propertiesDtype.load(new FileInputStream(path+"/conf/catalogs/"+cube.getCatalogName()+"/"+cube.getName()+"/dimensionstypes.mg"));
	    
	    if(oneDCuboids.isEmpty()){
		    for(Object a: propertiesD.values()){
		    	for(String aux: a.toString().split(":"))
		    		oneDCuboids.put(aux, new Object2ObjectOpenHashMap<String, IntCollection>());
		    }
	    }					
		
		Properties propertiesM = new Properties();
		
		propertiesM.load(new FileInputStream(path+"/conf/catalogs/"+cube.getCatalogName()+"/"+cube.getName()+"/measuresnames.mg"));
	    numM= propertiesM.size();
	    if(measureValues.isEmpty()){
		    for(Object a: propertiesM.values()){
		    	measureValues.put(a.toString(), new Int2DoubleOpenHashMap());
		    }	
	    }
		
		
		Properties propertiesDate = new Properties();
		String date="";
		
		propertiesDate.load(new FileInputStream(path+"/conf/configserver.mg"));
	    date = propertiesDate.getProperty("dateformat");		
		
		int lines = 100000+baseCuboid.size();
		int i=baseCuboid.size();
		int j=0;
		DateFormat dateFormat = new SimpleDateFormat(date);
		Calendar c = Calendar.getInstance();
		
		boolean flag = false;
									
		for(String oneF: files){
			flag=true;
			BufferedReader fileR = null;
			fileR= new BufferedReader(new FileReader(cube.getFullFileName()+oneF));
					
			text = null;			
							
			//header
			fileR.readLine();	
			
			//the first valid line
			text=fileR.readLine();
			
			while(text!=null){
				i++;
				if(i==lines){
					System.out.println("INDEXED " + i + " TUPLES...");
					lines+=100000;
				}					
				
				attributes = text.split("\\|",-1);	
				ObjectArrayList<Object> tuple = new ObjectArrayList<Object>();
				
				//tuples with size bigger than measures + dimensions
				//they are invalid tuples
				if(numD+numM!=attributes.length){
					//just do not compute the line and goes forward
					j++;						
				}else{
					//measures
					for(int w=0;w<numM;w++){
						//changing number with comma to number with point. Java implements 3.14 and not 3,14
						//attributes[w] cannot be null
						attributes[w]=attributes[w].replace(',', '.');
						if(!attributes[w].isEmpty())								
							measureValues.get(propertiesM.get(String.valueOf(w))).put(i, new Double(attributes[w]).doubleValue());
						//if the number is empty, MAGO adopts zero (0.0)
						else measureValues.get(propertiesM.get(String.valueOf(w))).put(i, new Double(0).doubleValue());		
						
					}
					//dimensions
					for(int w=numM;w<numD+numM;w++){
						//if a dimension value is empty, MAGO adopts 'NULL'
						if(attributes[w].isEmpty()){
							attributes[w]="NULL";
						}
						
													
						int auxx = w-numM;							
						
						if(propertiesDtype.getProperty(String.valueOf(auxx)).equals("alfa")){
							//attributes[w] = TMCleaner.removeSymbols(attributes[w]);
							
							Object2ObjectOpenHashMap<String, IntCollection> dimension = oneDCuboids.get(propertiesD.getProperty(String.valueOf(auxx)));
							
							if(dimension.get(attributes[w])==null){
								IntArrayList Tids = new IntArrayList();
								dimension.put(attributes[w], Tids);
							}
							dimension.get(attributes[w]).add(i);
							tuple.add(attributes[w]);
							
						}else if(propertiesDtype.getProperty(String.valueOf(auxx)).equals("text")){
							//any text data
							//attributes[w] = TMCleaner.removeSymbols(attributes[w]);
							
							Object2ObjectOpenHashMap<String, IntCollection> dimension = oneDCuboids.get(propertiesD.getProperty(String.valueOf(auxx)));
							
							//a text like an email or a document is separated into words
							//MAGO adopts space to separate a text into several words
							String[] words = attributes[w].split(" ");
							
							for(String word:words){
								//less than 2 characters are not considered as a valid word
								//if(word.length()>2){
									//word = Cleaner.cleanText(word);
									if(dimension.get(word)==null){
										IntArrayList Tids = new IntArrayList();
										dimension.put(word, Tids);
									}
									dimension.get(word).add(i);	
								//}
							}
							tuple.add(attributes[w]);
							//tuple.add("texto");
							
						}else if(propertiesDtype.getProperty(String.valueOf(auxx)).equals("time")){
							if(!attributes[w].equals("NULL")){
								
								Date inputDate = dateFormat.parse(attributes[w]);	
								c.setTime(inputDate);
								String[] timeNames = propertiesD.getProperty(String.valueOf(auxx)).split(":");
								if (timeNames==null || timeNames.length!=6)
									throw new MagoIndexCubeException("802", "the time dimension must have 6 members (year, month, day, hour, min and sec). method execute. class Index");
								
								//ano
								Object2ObjectOpenHashMap<String, IntCollection> dimension = oneDCuboids.get(timeNames[0]);
								
								if(dimension.get(String.valueOf(c.get(Calendar.YEAR)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.YEAR)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.YEAR))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.YEAR)));
								
								//mes
								dimension = oneDCuboids.get(timeNames[1]);									
								if(dimension.get(String.valueOf(c.get(Calendar.MONTH)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.MONTH)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.MONTH))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.MONTH)));

								//dia
								dimension = oneDCuboids.get(timeNames[2]);
								if(dimension.get(String.valueOf(c.get(Calendar.DAY_OF_MONTH)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.DAY_OF_MONTH)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.DAY_OF_MONTH))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.DAY_OF_MONTH)));

								//hora
								dimension = oneDCuboids.get(timeNames[3]);
								if(dimension.get(String.valueOf(c.get(Calendar.HOUR_OF_DAY)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.HOUR_OF_DAY)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.HOUR_OF_DAY))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.HOUR_OF_DAY)));
								
								//min
								dimension = oneDCuboids.get(timeNames[4]);
								if(dimension.get(String.valueOf(c.get(Calendar.MINUTE)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.MINUTE)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.MINUTE))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.MINUTE)));

								//seg
								dimension = oneDCuboids.get(timeNames[5]);
								if(dimension.get(String.valueOf(c.get(Calendar.SECOND)))==null){
									IntArrayList Tids = new IntArrayList();
									dimension.put(String.valueOf(c.get(Calendar.SECOND)), Tids);
								}
								dimension.get(String.valueOf(c.get(Calendar.SECOND))).add(i);
								tuple.add(String.valueOf(c.get(Calendar.SECOND)));

							}//end if
							else{
								//put year, month, day, hour, etc.. as -1, indicating an invalid date
								for(int u=0; u<6; u++)
									tuple.add("-1");
							}
						}//end if
					}//end for
					
					String[] tupleFinal = new String[tuple.size()];
					tuple.toArray(tupleFinal);
										
					baseCuboid.put(i, tupleFinal);
					
				}//end else	
				
				attributes=null;
				tuple.clear();
				tuple =null;
				
				text=fileR.readLine();
								
				if(stop.isStop()==true) {
					attributes=null;
					if(tuple!=null)
						tuple.clear();
					tuple =null;
					fileR.close();
					fileR=null;								
					text = null;
					propertiesD=null;
					propertiesDtype=null;
					propertiesM =null;
					propertiesDate=null;
					
					throw new MagoIndexCubeException("803", "MiDAS message: Cube stopped during an index or update.");
					
				}
				
			}//end while
			
			if (fileR!=null) fileR.close();
			fileR=null;
						
			text = null;
			
			if(stop.isStop()==true) {
				attributes=null;
				if(fileR!=null)
					fileR.close();
				fileR=null;								
				text = null;
				propertiesD=null;
				propertiesDtype=null;
				propertiesM =null;
				propertiesDate=null;
				throw new MagoIndexCubeException("803", "MiDAS message: Cube stopped during an index or update.");
				
			}
			
		}//end for of files
		
		propertiesD=null;
		propertiesDtype=null;
		propertiesM =null;
		propertiesDate=null;
		//System.out.println("base cuboid and one D cuboids generated...");
		
		if(flag){
			if(stop.isStop()==false){
				//String path = DriverConfig.getPah();						
				new SerializeObjectImpl<Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, IntCollection>>>().serializeToFile(oneDCuboids, path+"/base/indexed/"+cube.getCatalogName()+"/"+cube.getName()+"/", "onedcuboids.mago");
				new SerializeObjectImpl<Int2ObjectOpenHashMap<String[]>>().serializeToFile(baseCuboid, path+"/base/indexed/"+cube.getCatalogName()+"/"+cube.getName()+"/", "basecuboid.mago");
				new SerializeObjectImpl<Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap>>().serializeToFile(measureValues, path+"/base/indexed/"+cube.getCatalogName()+"/"+cube.getName()+"/", "measurevalues.mago");
				new SerializeObjectImpl<Set<String>>().serializeToFile(inputs, path+"/base/indexed/"+cube.getCatalogName()+"/"+cube.getName()+"/", "inputs.mago");
				System.out.println("number of tuples: " + i);
				System.out.println("number of invalid tuples: " + j);
				System.out.println("time to index: " + (time-System.nanoTime())/1000000000 + "seconds");
				
			}else{
				attributes=null;
				text = null;
				propertiesD=null;
				propertiesDtype=null;
				propertiesM =null;
				propertiesDate=null;
				throw new MagoIndexCubeException("803", "MiDAS message: Cube stopped during an index or update.");
				
			}
		}
		files.clear();
		files=null;
		filesF=null;		
	}
	
}
