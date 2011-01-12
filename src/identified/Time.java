package identified;

class Time{
    public static double now(){
        double time = System.currentTimeMillis();
        return time/1000.0;
    }
}
