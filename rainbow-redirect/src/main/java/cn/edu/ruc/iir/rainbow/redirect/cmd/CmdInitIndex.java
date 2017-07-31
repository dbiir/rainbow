package cn.edu.ruc.iir.rainbow.redirect.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CmdInitIndex implements Command
{
    @Override
    public void setReceiver(Receiver receiver)
    {

    }

    @Override
    public void execute(Properties params)
    {
        try (BufferedReader reader = new BufferedReader(new FileReader("")))
        {
            // create the dupped column order.
            List<String> columnOrder = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] tokens = line.split("\t");
                columnOrder.add(tokens[0]);
            }

            // TODO: build and cache the index
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();// new buffer
        } catch (IOException e)
        {
            e.printStackTrace();//close
        }
    }
}
