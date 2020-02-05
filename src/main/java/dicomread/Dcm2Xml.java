/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is part of dcm4che, an implementation of DICOM(TM) in
 * Java(TM), hosted at https://github.com/dcm4che.
 *
 * The Initial Developer of the Original Code is
 * Agfa Healthcare.
 * Portions created by the Initial Developer are Copyright (C) 2011
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * See @authors listed below
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

package dicomread;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.cli.*;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.io.ContentHandlerAdapter;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.io.SAXWriter;
/*import org.dcm4che3.tool.common.CLIUtils;
 */

/**
 * @author Gunter Zeilinger <gunterze@gmail.com>
 */

public class Dcm2Xml implements java.io.Serializable {
    private static final String XML_1_0 = "1.0";
    private static final String XML_1_1 = "1.1";

    private static ResourceBundle rb =
            ResourceBundle.getBundle("org.dcm4che3.tool.dcm2xml.messages");

    private String xsltURL;
    private boolean indent = false;
    private boolean includeKeyword = true;
    private boolean includeNamespaceDeclaration = false;
    //private IncludeBulkData includeBulkData = IncludeBulkData.URI;
    private IncludeBulkData includeBulkData = IncludeBulkData.NO;
    private boolean catBlkFiles = false;
    private String blkFilePrefix = "blk";
    private String blkFileSuffix;
    private File blkDirectory;
    private Attributes blkAttrs;
    private String xmlVersion = XML_1_0;

    public final void setXSLTURL(String xsltURL) {
        this.xsltURL = xsltURL;
    }

    public final void setIndent(boolean indent) {
        this.indent = indent;
    }

    public final void setIncludeKeyword(boolean includeKeyword) {
        this.includeKeyword = includeKeyword;
    }

    public final void setIncludeNamespaceDeclaration(boolean includeNamespaceDeclaration) {
        this.includeNamespaceDeclaration = includeNamespaceDeclaration;
    }

    public final void setIncludeBulkData(IncludeBulkData includeBulkData) {
        this.includeBulkData = includeBulkData;
    }

    public final void setConcatenateBulkDataFiles(boolean catBlkFiles) {
        this.catBlkFiles = catBlkFiles;
    }

    public final void setBulkDataFilePrefix(String blkFilePrefix) {
        this.blkFilePrefix = blkFilePrefix;
    }

    public final void setBulkDataFileSuffix(String blkFileSuffix) {
        this.blkFileSuffix = blkFileSuffix;
    }

    public final void setBulkDataDirectory(File blkDirectory) {
        this.blkDirectory = blkDirectory;
    }

    public final void setBulkDataAttributes(Attributes blkAttrs) {
        this.blkAttrs = blkAttrs;
    }

    public final void setXMLVersion(String xmlVersion) {
        this.xmlVersion = xmlVersion;
    }

    @SuppressWarnings("static-access")
    private static void addBulkdataOptions(Options opts) {
        OptionGroup group = new OptionGroup();
        group.addOption(OptionBuilder
                .withLongOpt("no-bulkdata")
                .withDescription(rb.getString("no-bulkdata"))
                .create("B"));
        group.addOption(OptionBuilder
                .withLongOpt("with-bulkdata")
                .withDescription(rb.getString("with-bulkdata"))
                .create("b"));
        opts.addOptionGroup(group);
        opts.addOption(OptionBuilder
                .withLongOpt("blk-file-dir")
                .hasArg()
                .withArgName("directory")
                .withDescription(rb.getString("blk-file-dir"))
                .create("d"));
        opts.addOption(OptionBuilder
                .withLongOpt("blk-file-prefix")
                .hasArg()
                .withArgName("prefix")
                .withDescription(rb.getString("blk-file-prefix"))
                .create());
        opts.addOption(OptionBuilder
                .withLongOpt("blk-file-suffix")
                .hasArg()
                .withArgName("suffix")
                .withDescription(rb.getString("blk-file-dir"))
                .create());
        opts.addOption("c", "cat-blk-files", false,
                rb.getString("cat-blk-files"));
        opts.addOption(OptionBuilder
                .withLongOpt("blk-spec")
                .hasArg()
                .withArgName("xml-file")
                .withDescription(rb.getString("blk-spec"))
                .create("X"));
    }

    private static String toURL(String fileOrURL) {
        try {
            new URL(fileOrURL);
            return fileOrURL;
        } catch (MalformedURLException e) {
            return new File(fileOrURL).toURI().toString();
        }
    }

    private static void configureBulkdata(Dcm2Xml dcm2xml, CommandLine cl)
            throws Exception {
        if (cl.hasOption("b")) {
            dcm2xml.setIncludeBulkData(IncludeBulkData.YES);
        }
        if (cl.hasOption("B")) {
            dcm2xml.setIncludeBulkData(IncludeBulkData.NO);
        }
        if (cl.hasOption("blk-file-prefix")) {
            dcm2xml.setBulkDataFilePrefix(
                    cl.getOptionValue("blk-file-prefix"));
        }
        if (cl.hasOption("blk-file-suffix")) {
            dcm2xml.setBulkDataFileSuffix(
                    cl.getOptionValue("blk-file-suffix"));
        }
        if (cl.hasOption("d")) {
            File tempDir = new File(cl.getOptionValue("d"));
            dcm2xml.setBulkDataDirectory(tempDir);
        }
        dcm2xml.setConcatenateBulkDataFiles(cl.hasOption("c"));
        if (cl.hasOption("X")) {
            dcm2xml.setBulkDataAttributes(
                    parseXML(cl.getOptionValue("X")));
        }
    }

    private static Attributes parseXML(String fname) throws Exception {
        Attributes attrs = new Attributes();
        ContentHandlerAdapter ch = new ContentHandlerAdapter(attrs);
        SAXParserFactory f = SAXParserFactory.newInstance();
        SAXParser p = f.newSAXParser();
        p.parse(new File(fname), ch);
        return attrs;
    }

    private static String fname(List<String> argList) throws ParseException {
        int numArgs = argList.size();
        if (numArgs == 0)
            throw new ParseException(rb.getString("missing"));
        if (numArgs > 1)
            throw new ParseException(rb.getString("too-many"));
        return argList.get(0);
    }

    public void convert(DicomInputStream dis, OutputStream out) throws IOException,
            TransformerConfigurationException {
        dis.setIncludeBulkData(includeBulkData);
        //if (blkAttrs != null)
        //dis.setBulkDataDescriptor(BulkDataDescriptor.valueOf(blkAttrs));
        //dis.setBulkDataDirectory(blkDirectory);
        //dis.setBulkDataFilePrefix(blkFilePrefix);
        //dis.setBulkDataFileSuffix(blkFileSuffix);
        //dis.setConcatenateBulkDataFiles(catBlkFiles);
        TransformerHandler th = getTransformerHandler();
        Transformer t = th.getTransformer();
        t.setOutputProperty(OutputKeys.INDENT, indent ? "yes" : "no");
        t.setOutputProperty(OutputKeys.VERSION, xmlVersion);
        th.setResult(new StreamResult(out));
        SAXWriter saxWriter = new SAXWriter(th);
        saxWriter.setIncludeKeyword(includeKeyword);
        saxWriter.setIncludeNamespaceDeclaration(includeNamespaceDeclaration);
        dis.setDicomInputHandler(saxWriter);
        dis.readDataset(-1, -1);
    }

    public void convert(File dicomFile, OutputStream out) throws IOException,
            TransformerConfigurationException {
        DicomInputStream dis = new DicomInputStream(dicomFile);
        try {
            convert(dis, out);
        } finally {
            dis.close();
        }
    }

    public void convert(File dicomFile, File xmlFile) throws IOException,
            TransformerConfigurationException {
        OutputStream out = new BufferedOutputStream(new FileOutputStream(xmlFile));
        try {
            convert(dicomFile, out);
        } finally {
            out.close();
        }
    }

    private TransformerHandler getTransformerHandler()
            throws TransformerConfigurationException, IOException {
        SAXTransformerFactory tf = (SAXTransformerFactory)
                TransformerFactory.newInstance();
        if (xsltURL == null)
            return tf.newTransformerHandler();

        TransformerHandler th = tf.newTransformerHandler(
                new StreamSource(xsltURL));
        return th;
    }
}