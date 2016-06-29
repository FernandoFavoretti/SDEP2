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
	
	public static int[] translateMesSemana(){
		int[] resp = new int[2];
		resp[0] = 18;
		resp[1] = 22;
		return resp;
	}
	
	public static String translateAgregacao(String agregacao){
		String resp="";
		if(agregacao.equals("mesano")){
			resp = "Meses por ano";
		}
		if(agregacao.equals("messem")){
			resp = "Semanas por meses";
		}
		return resp;
	}
	
	public static String translateOperacoes(String ops){
		String resp ="";
		if(ops.equals("media")){
			resp = "Média";
		}
		if(ops.equals("max")){
			resp = "Valor Máximo";
		}
		if(ops.equals("min")){
			resp = "Valor Mínimo";
		}
		if(ops.equals("var")){
			resp = "Variância";
		}
		if(ops.equals("desvio")){
			resp = "Desvio Padrão";
		}
		if(ops.equals("mq")){
			resp = "Mínimos Quadrados";
		}
		
		return resp;
	}
	
	public static String translateNomeDado(String dado){
		String resp ="";
		if(dado.equals("temp")){
			resp = "temperatura";
		}
		if(dado.equals("ca")){
			resp = "Pontos de Condensação da Água";
		}
		if(dado.equals("pn")){
			resp = "Pressão no nível do mar";
		}
		if(dado.equals("pr")){
			resp = "Pressão";
		}
		if(dado.equals("vb")){
			resp = "Visibilidade";
		}
		if(dado.equals("vv")){
			resp = "Velocidade do Vento";
		}
		if(dado.equals("rv")){
			resp = "Rajadas de Vento";
		}
		if(dado.equals("pc")){
			resp = "Precipitação";
		}
		if(dado.equals("nv")){
			resp = "neve";
		}
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
		if(dado.equals("pc")){
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
