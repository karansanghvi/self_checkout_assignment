package com.school.selfcheckout.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Machine-checked statement of the service-based architecture.
 *
 * Maven already stops a service from compiling against another service --
 * none declares one as a dependency. This test puts all five on a single
 * classpath anyway, so the rules can be stated and checked in one place,
 * including the ones Maven cannot see: which shared data each service is
 * coupled to, and that no service makes outbound HTTP calls.
 *
 * Packages:
 *
 *   service.{catalog,checkout,inventory,analytics,admin}   the deployable services
 *   data.{catalog,inventory,checkout,analytics,system}     shared database access, one library per domain
 *   contract, domain, web                                  the common library
 */
class ServiceBoundariesTest {

    private static final String BASE = "com.school.selfcheckout";

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages(BASE);

    /**
     * The coupling matrix: each service and the data libraries it may touch.
     * This is the whole answer to "how is shared database access distributed"
     * -- and the one place to look to see which services a schema change hits.
     */
    private static final Map<String, List<String>> ALLOWED_DATA = Map.of(
            "catalog", List.of("catalog", "system"),
            "checkout", List.of("checkout", "inventory", "catalog", "system"),
            "inventory", List.of("inventory"),
            "analytics", List.of("analytics", "catalog", "system"),
            "admin", List.of("system"));

    private static final List<String> DATA_LIBRARIES =
            List.of("catalog", "inventory", "checkout", "analytics", "system");

    /** Guards against the rules below silently passing on an empty import. */
    @Test
    void everyServiceWasActuallyImported() {
        for (String service : ALLOWED_DATA.keySet()) {
            assertThat(CLASSES.stream().anyMatch(c -> c.getPackageName().startsWith(BASE + ".service." + service)))
                    .as("classes of %s-service on the classpath", service)
                    .isTrue();
        }
    }

    @Test
    void servicesDoNotDependOnEachOther() {
        slices().matching(BASE + ".service.(*)..")
                .should().notDependOnEachOther()
                .because("services coordinate only through the shared database")
                .check(CLASSES);
    }

    @Test
    void servicesTouchOnlyTheirDeclaredData() {
        for (Map.Entry<String, List<String>> entry : ALLOWED_DATA.entrySet()) {
            String[] forbidden = DATA_LIBRARIES.stream()
                    .filter(lib -> !entry.getValue().contains(lib))
                    .map(lib -> BASE + ".data." + lib + "..")
                    .toArray(String[]::new);
            if (forbidden.length == 0) {
                continue;
            }
            noClasses().that().resideInAPackage(BASE + ".service." + entry.getKey() + "..")
                    .should().dependOnClassesThat().resideInAnyPackage(forbidden)
                    .because(entry.getKey() + "-service's data coupling is " + entry.getValue())
                    .check(CLASSES);
        }
    }

    /** "Services should not communicate directly": none of them holds an HTTP client. */
    @Test
    void servicesMakeNoOutboundHttpCalls() {
        noClasses().that().resideInAPackage(BASE + ".service..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "java.net.http..",
                        "org.springframework.web.client..",
                        "org.springframework.web.reactive.function.client..",
                        "org.apache.hc..")
                .because("a service may not call another service")
                .check(CLASSES);
    }

    /** The data libraries are leaves: they know SQL and records, not services or HTTP. */
    @Test
    void dataLibrariesDoNotReachUpwards() {
        noClasses().that().resideInAPackage(BASE + ".data..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        BASE + ".service..", BASE + ".web..", BASE + ".contract..",
                        "org.springframework.web..", "org.springframework.http..")
                .check(CLASSES);
    }

    /**
     * Domain data libraries are independent of each other. The one exception
     * is system, which every library may use for the reset epoch.
     */
    @Test
    void domainDataLibrariesAreIndependent() {
        List<String> domains = DATA_LIBRARIES.stream().filter(lib -> !lib.equals("system")).toList();
        for (String lib : domains) {
            String[] others = domains.stream()
                    .filter(other -> !other.equals(lib))
                    .map(other -> BASE + ".data." + other + "..")
                    .toArray(String[]::new);
            noClasses().that().resideInAPackage(BASE + ".data." + lib + "..")
                    .should().dependOnClassesThat().resideInAnyPackage(others)
                    .check(CLASSES);
        }
    }

    /** The common library is the bottom of the stack. */
    @Test
    void commonDependsOnNothingOfOurs() {
        noClasses().that().resideInAnyPackage(BASE + ".contract..", BASE + ".domain..", BASE + ".web..")
                .should().dependOnClassesThat().resideInAnyPackage(BASE + ".service..", BASE + ".data..")
                .check(CLASSES);
    }

    /** Status codes are chosen in exactly one place, {@code GlobalExceptionHandler}. */
    @Test
    void serviceLogicDoesNotChooseHttpStatuses() {
        noClasses().that().resideInAPackage(BASE + ".service..")
                .and().haveSimpleNameNotEndingWith("Controller")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.http..", "jakarta.servlet..")
                .because("choosing HTTP status codes is GlobalExceptionHandler's job")
                .check(CLASSES);
    }
}
