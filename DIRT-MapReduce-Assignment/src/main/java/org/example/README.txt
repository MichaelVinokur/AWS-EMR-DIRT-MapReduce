job1:
input:
experience	experience/VB/ROOT/0 patients/NNS/nsubj/3 experience/VB/ROOT/0 difficulty/NN/dobj/1	5
experience	experience/VB/ROOT/0 patients/NNS/nsubj/3 experience/VB/ROOT/0 problem/NN/dobj/1	3
experience	experience/VB/ROOT/0 doctors/NNS/nsubj/3 experience/VB/ROOT/0 difficulty/NN/dobj/1	2
cease	cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 instant/NN/pobj/2	56
cease	cease/VB/ccomp/0 for/IN/prep/1 a/DT/det/4 moment/NN/pobj/2	10

output:
*	X	doctor	2
*	X	patient	8
*	Y	difficulti	7
*	Y	instant	56
*	Y	moment	10
*	Y	problem	3
TOTAL	X	10
TOTAL	Y	76
ceas#prep_for#Y	Y	*	66
ceas#prep_for#Y	Y	instant	56
ceas#prep_for#Y	Y	moment	10
experi#X#Y	X	*	10
experi#X#Y	X	doctor	2
experi#X#Y	X	patient	8
experi#X#Y	Y	*	10
experi#X#Y	Y	difficulti	7
experi#X#Y	Y	problem	3

explanation run:
cease	cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 instant/NN/pobj/2	56
DirtMapper - (no X)
p = ceas#prep_for#Y, slot = Y, word = instant, freq = 56
Then it outputs 4 “count facts”:
“instant filled Y for this path” - ceas#prep_for#Y    Y    instant    56
total Y fillers for this path (all words) - ceas#prep_for#Y    Y    *          56
“instant appeared in Y slot in general (any path)” - *               Y    instant    56
total Y events in general - TOTAL           Y            56
DirtReducer - Just adds numbers if the same key appears again. (Example: if another line also produced * Y instant 10, reducer makes it 66.)
So after Job 1 you have counts like: c(p,Y,instant), c(p,Y,*), c(*,Y,instant), TOTAL Y

job2 - key: offset in file val: *	X	doctor 2
MI calculation  -  the mutual information between a path slot and its filler (4.3 in the file)
output:
ceas#prep_for#Y   Y   moment    0.1410785982599056
ceas#prep_for#Y   Y   instant  0.1410785982599056
experi#X#Y        X   doctor   0.0
experi#X#Y        X   patient  0.0
experi#X#Y        Y   difficulti 2.028148247292285
experi#X#Y        Y   problem   2.028148247292285



 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 3) return;

            String headWord = tokens[0];
            String stemmedHead = applyStemming(headWord); //checks if part of auxileryVerbs and applying stemmer.
            if (stemmedHead == null) return;

            int freq;
            try { freq = Integer.parseInt(tokens[2].trim()); }
            catch (Exception e) { return; }

            boolean headIsVerb = false;
            String prep = null;      // e.g. "for" - IN
            String xNoun = null; // subject = נושא
            String yNoun = null; // object = מושא

            String[] syntacticNgram = tokens[1].split("\\s+"); // cease/VB/ccomp/0  for/IN/prep/1  some/DT/det/4  time/NN/pobj/2
            for(String s: syntacticNgram){
                String [] sBreak = s.split("/");
                if (sBreak.length < 2) continue;
                String word = sBreak[0];
                String pos = sBreak[1];
                String dep="";
                if(sBreak.length>2) {
                    dep = sBreak[2];
                }

                if(word.equals(headWord)){
                    if(pos.startsWith("VB")){ headIsVerb=true;}
                    else return; // if head not verb ignore
                }
                if(pos.startsWith("NN") || pos.startsWith("NNS") || pos.startsWith("NNP") || pos.startsWith("NNPS")){
                    String stem = applyStemming(word);
                    if (stem == null) continue;

                    if (dep.equals("nsubj") && xNoun == null) {
                        xNoun = stem;
                    }
                    if ((dep.equals("dobj") || dep.equals("pobj")) && yNoun == null) {
                        yNoun = stem;
                    }
                }
                if (prep == null && (pos.equals("IN") || pos.equals("TO"))){
                    prep = word.toLowerCase();
                }
            }
            if (!headIsVerb) return;
            if (yNoun == null) return;
            String p;
            if (prep != null) p = stemmedHead + "#prep_" + prep + "#Y";
            else p = stemmedHead + "#Y";

            if (xNoun == null) {
                // Y-only path
                String pY = (prep != null) ? stemmedHead + "#prep_" + prep + "#Y" : stemmedHead + "#Y";
                emitCounts(context, pY, "Y", yNoun, freq);
            } else {
                // X-Y path
                String pXY = (prep != null)
                        ? stemmedHead + "#X#prep_" + prep + "#Y"
                        : stemmedHead + "#X#Y";
                emitCounts(context, pXY, "X", xNoun, freq);
                emitCounts(context, pXY, "Y", yNoun, freq);
            }
        }