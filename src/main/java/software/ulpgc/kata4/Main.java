package software.ulpgc.kata4;

import software.ulpgc.kata4.control.ImportCommand;
import software.ulpgc.kata4.ui.ImportDialog;

import java.io.File;

public class Main {
    public static void main(String[] args) {
        new ImportCommand(importDialog()).execute();
    }

    private static ImportDialog importDialog() {
        return () -> new File("D:\\Uni\\IS2\\title.basics.tsv.gz");
    }
}
