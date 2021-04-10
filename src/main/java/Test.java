

public class Test {
    public static void main(String[] args){
        IdealistaToMongoDB idea = new IdealistaToMongoDB();
        try {
            idea.idealistaToDB();
        }catch (java.io.IOException e){
            e.printStackTrace();
        }
    }

}
