//Created by ODI Studio
import org.apache.commons.io.FilenameUtils

def pwFilePath;

def readCredentials() {
    Map<String, String> map = new HashMap<String, String>()
    inReader = new BufferedReader(new FileReader(pwFilePath));
    String line = "";
    while ((line = inReader.readLine()) != null) {
        parts = []
        parts = line.split(":");
        map.put(parts[0], parts[1]);
    }
    inReader.close();
    return map
}

def getValue(key){
  credentials = readCredentials()
  return credentials[key]
}
