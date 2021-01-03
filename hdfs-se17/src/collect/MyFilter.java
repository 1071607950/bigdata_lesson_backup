package collect;

import java.io.File;
import java.io.FilenameFilter;

public class MyFilter implements FilenameFilter {

	@Override
	public boolean accept(File dir, String name) {
		if(name.startsWith("test.log."))
			return true;
		return false;
	}

}
