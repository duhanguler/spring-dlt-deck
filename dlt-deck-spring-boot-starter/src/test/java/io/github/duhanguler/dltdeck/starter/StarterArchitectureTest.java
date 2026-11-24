package io.github.duhanguler.dltdeck.starter;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import io.github.duhanguler.dltdeck.starter.kafka.SafeSpringHeaderExtractor;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@AnalyzeClasses(
        packages = "io.github.duhanguler.dltdeck.starter",
        importOptions = ImportOption.DoNotIncludeTests.class)
class StarterArchitectureTest {

    @ArchTest
    static final ArchRule starterNeverRegistersGlobalWebMvcConfigurer = noClasses()
            .should()
            .implement(WebMvcConfigurer.class);

    @ArchTest
    static final ArchRule starterNeverUsesThreadLocalInFavorOfScopedValue = noClasses()
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("java.lang.ThreadLocal");

    @ArchTest
    static final ArchRule onlySafeSpringHeaderExtractorMayUseObjectInputStream = noClasses()
            .that()
            .areNotAssignableTo(SafeSpringHeaderExtractor.class)
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("java.io.ObjectInputStream");
}
