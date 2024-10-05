import java.util.*;;
public class isAnagram{

    Map<Character, Integer> set1 = new HashMap<>();
    Map<Character, Integer> set2 = new HashMap<>();
    Map<Character,Boolean> set3 = new HashMap<>();
    public boolean verdict=false;

    public String isAnagram(String str1, String str2) {
        
        Integer score =0;
        for (char c : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
             set3.put(c,false);
        }
       
        char[] charArray1 = str1.toLowerCase().toCharArray();
        char[] charArray2 = str2.toLowerCase().toCharArray();
      

        for(char c : charArray1) {
            if(set1.containsKey(c)) {
                     set1.put(c, set1.get(c) + 1);

                }
            else {
                set1.put(c, 1);
            }
        }
         for(char c : charArray2) {
            if(set2.containsKey(c)) {
                     set2.put(c, set2.get(c) + 1);
                }
            else {
                set2.put(c, 1);
            }
        }


        for(char c : charArray1) {
            if(set2.containsKey(c)){            
                
            Integer element1 = set1.get(c);
            Integer element2 = set2.get(c);
            
            if(set3.containsKey(c) && set3.get(c)!=true)  score = score+ Math.min(element1,element2);
            set3.put(c,true);
            }   
        }

        Float intersection1=(float) score/str1.length();
        Float intersection2=(float) score/str2.length();
        if(intersection1>0.5 && intersection2>0.5) verdict=true;
        // System.out.println((verdict));

            
            



        return "Matched with "+Integer.toOctalString(score)+" score with intersecton of "+(intersection1)+" and "+intersection2+". Verdict is "+(intersection1>0.5 && intersection2>0.5?true:false);
    }





}


