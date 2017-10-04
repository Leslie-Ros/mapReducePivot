
  @Override
  public void map(IntWritable key, Text value, Context context)
          throws IOException, InterruptedException {
            int index = key.get();
            IntWritable keyMap = new IntWritable();
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            while (itr.hasMoreTokens()){
              word.set(itr.nextToken());
              keyMap.set(index);
              context.write(keyMap, word);
              index =+ 1;
            }
          }

@Override
public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
          Text output = new Text();
          String ligne ='';
          for (Text val : values) {
            ligne += val.toString() + ',';
          }
          ligne = ligne.substring(0, ligne.length() - 1)
          output.set(ligne);
          context.write(key,output);
        }
