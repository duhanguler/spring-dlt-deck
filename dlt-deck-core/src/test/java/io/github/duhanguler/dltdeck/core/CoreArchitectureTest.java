package io.github.duhanguler.dltdeck.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(
        packages = "io.github.duhanguler.dltdeck.core",
        importOptions = ImportOption.DoNotIncludeTests.class)
class CoreArchitectureTest {

    @ArchTest
    static final ArchRule coreHasZeroSpringServletJdbcOrJpaDependencies = noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                    "org.springframework..",
                    "jakarta.servlet..",
                    "javax.sql..",
                    "jakarta.persistence..",
                    "org.apache.kafka..");

    @ArchTest
    static final ArchRule coreNeverUsesThreadLocalInFavorOfScopedValue = noClasses()
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("java.lang.ThreadLocal");

    @ArchTest
    static final ArchRule coreNeverUsesObjectInputStream = noClasses()
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("java.io.ObjectInputStream");
}
