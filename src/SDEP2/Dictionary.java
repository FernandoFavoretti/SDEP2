package SDEP2;

public class Dictionary {
	
	public static int[] translateAno(){
		int[] resp = new int[2];
		resp[0] = 14;
		resp[1] = 18;
		return resp;
	} 
	
	public static int[] translateMes(){
		int[] resp = new int[2];
		resp[0] = 18;
		resp[1] = 20;
		return resp;
	}
	
	public static int[] translateSemana(){
		int[] resp = new int[2];
		resp[0] = 20;
		resp[1] = 22;
		return resp;
	}
	public static int[] translateMesAno(){
		int[] resp = new int[2];
		resp[0] = 14;
		resp[1] = 20;
		return resp;
	}
	
	public static int[] translateDado(String dado){
		int[] resp = new int[2];
		
		if(dado.equals("temp")){
			resp[0] = 24;
			resp[1] = 30;
		}
		if(dado.equals("ca")){
			resp[0] = 35;
			resp[1] = 41;
		}
		if(dado.equals("pn")){
			resp[0] = 46;
			resp[1] = 52;
		}
		if(dado.equals("pr")){
			resp[0] = 57;
			resp[1] = 63;
		}
		if(dado.equals("vb")){
			resp[0] = 68;
			resp[1] = 73;
		}
		if(dado.equals("vv")){
			resp[0] = 78;
			resp[1] = 83;
		}
		if(dado.equals("rv")){
			resp[0] = 95;
			resp[1] = 100;
		}
		if(dado.equals("pr")){
			resp[0] = 118;
			resp[1] = 123;
		}
		if(dado.equals("nv")){
			resp[0] = 125;
			resp[1] = 130;

		}
		return resp;
}
	
	
	
	

}
