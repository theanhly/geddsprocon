package de.tuberlin.mcc.geddsprocon.geddsproconcore.common;

import java.io.*;

public class SerializationTool {

    public static byte[] serialize(Serializable ser) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(ser);
            return bos.toByteArray();
        } catch(IOException ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public static Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch(ClassNotFoundException ex) {
            System.err.println("ClassNotFoundException: Class not found.");
            System.err.println(ex.toString());
        }catch (IOException ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
