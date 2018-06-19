package com.hadoop.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class Create_Phone {

    public static void main(String[] args) throws IOException {
      String [] starts ={"130","131","132","133","139","151","152","153","156","158","159","183","185","186"};
        StringBuffer buffer = new StringBuffer();
        FileOutputStream fos = new FileOutputStream("e:/phone_info.txt");
        for (int i = 1; i <= 2000000; i++) {
            int upFlow = (int)((Math.random()*9+1)*1000);
            int end=(int)((Math.random()*9+1)*10000000);
            String start=starts[(int)(Math.random()*starts.length-1)];
            buffer=buffer.append(i+" ").append(start+end).append(" ").append(UUID.randomUUID().toString().substring(0,10))
                    .append(" ").append(upFlow+" ").append(upFlow/((upFlow%4)+1));
            fos.write(buffer.toString().getBytes());
            fos.write("\r\n".getBytes());
            buffer=new StringBuffer();
            if (i%5==0){
                upFlow = (int)((Math.random()*9+1)*1000);
                buffer=buffer.append(++i+" ").append(start+end).append(" ").append(UUID.randomUUID().toString().substring(0,10))
                        .append(" ").append(upFlow+" ").append(upFlow/((upFlow%4)+1));
                System.out.println(buffer);
                fos.write(buffer.toString().getBytes());
                fos.write("\r\n".getBytes());
                buffer=new StringBuffer();
            }
            if (i%3==0){
                upFlow = (int)((Math.random()*9+1)*1000);
                buffer=buffer.append(++i+" ").append(start+end).append(" ").append(UUID.randomUUID().toString().substring(0,10))
                        .append(" ").append(upFlow+" ").append(upFlow/((upFlow%4)+1));
                System.out.println(buffer);
                fos.write(buffer.toString().getBytes());
                fos.write("\r\n".getBytes());
                buffer=new StringBuffer();
            }
        }
        fos.close();
    }
}
