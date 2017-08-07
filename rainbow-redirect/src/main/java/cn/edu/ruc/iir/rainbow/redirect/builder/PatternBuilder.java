package cn.edu.ruc.iir.rainbow.redirect.builder;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.redirect.domain.AccessPattern;
import cn.edu.ruc.iir.rainbow.redirect.domain.ColumnSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by hank on 2017/7/31.
 */
public class PatternBuilder
{
    private PatternBuilder()
    {

    }

    public static List<AccessPattern> build (File workloadFile)
            throws IOException, ColumnNotFoundException
    {
        List<AccessPattern> patterns = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(workloadFile)))
        {
            Set<ColumnSet> existingColumnSets = new HashSet<>();
            String line;
            int qid = 0;
            while ((line = reader.readLine()) != null)
            {
                String[] columnReplicas = line.split("\t")[2].split(",");
                AccessPattern pattern = new AccessPattern();
                for (String columnReplica : columnReplicas)
                {
                    pattern.addColumnReplica(columnReplica);
                }

                ColumnSet columnSet = pattern.getColumnSet();

                if (! existingColumnSets.contains(columnSet))
                {
                    patterns.add(pattern);
                    existingColumnSets.add(columnSet);
                }

            }
        } catch (IOException e)
        {
            throw e;
        }
        return patterns;

    }
}
