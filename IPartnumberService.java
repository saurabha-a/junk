
package com.unilog.cx1.pim.commons.service;

import java.util.Map;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;

public interface IPartnumberService {

    void doImport(Import anImport, PimDataObject qdo);

    Map<String, String> getPartnumberTypes();

}
