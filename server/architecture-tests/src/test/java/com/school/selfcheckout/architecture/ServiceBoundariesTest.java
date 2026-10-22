package com.school.selfcheckout.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Machine-checked statement of the microservice boundaries.
 *
 * Maven already stops a service from compiling against another service --
 * none declares one as a dependency, and there are no shared data libraries
 * left to couple them. This test puts all five on a single classpath anyway,
 * so the rules about <i>how</i> they may talk can be stated and checked in
 * one place:
 *
 * <pre>
 *   checkout  --HTTP-->  inventory   POST /internal/stock-commits   (the completion saga)
 *   checkout  --HTTP-->  catalog     GET /items                     (catalog replica)
 *   inventory --HTTP-->  catalog     GET /items                     (reseed at reset)
 *   analytics --HTTP-->  catalog     GET /items                     (catalog replica)
 *   admin     --HTTP-->  all four    POST /internal/reset           (reset fan-out)
 *   checkout  --Kafka-> analytics    scan-events topic              (outbox relay)
 * </pre>
 *
 * Packages:
 *
 *   service.{catalog,checkout,inventory,analytics,admin}   the deployable services
 *   contract, domain, web, client                          the common library
 */
class ServiceBoundariesTest {

    private static final String BASE = "com.school.selfcheckout";

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages(BASE);

    private static final List<String> SERVICES = List.of("catalog", "checkout", "inventory", "analytics", "admin");

    private static final String[] HTTP_CLIENTS = {
            "java.net.http..",
            "org.springframework.web.client..",
            "org.springframework.web.reactive.function.client..",
            "org.apache.hc..",
            BASE + ".client.."};

    private static String service(String name) {
        return BASE + ".service." + name + "..";
    }

    /** Guards against the rules below silently passing on an empty import. */
    @Test
    void everyServiceWasActuallyImported() {
        for (String service : SERVICES) {
            assertThat(CLASSES.stream().anyMatch(c -> c.getPackageName().startsWith(BASE + ".service." + service)))
                    .as("classes of %s-service on the classpath", service)
                    .isTrue();
        }
    }

    @Test
    void servicesDoNotDependOnEachOther() {
        slices().matching(BASE + ".service.(*)..")
                .should().notDependOnEachOther()
                .because("services talk over HTTP and Kafka, never through each other's classes")
                .check(CLASSES);
    }

    /** catalog-service is a leaf: everyone reads from it, it calls no one. */
    @Test
    void catalogCallsNoOtherService() {
        noClasses().that().resideInAPackage(service("catalog"))
                .should().dependOnClassesThat().resideInAnyPackage(HTTP_CLIENTS)
                .because("catalog-service depends on nothing but its own database")
                .check(CLASSES);
    }

    /** admin-service orchestrates; it owns no data, so it has no way to reach a database. */
    @Test
    void adminHasNoDatabase() {
        noClasses().that().resideInAPackage(service("admin"))
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.jdbc..", "javax.sql..", "java.sql..")
                .because("admin-service resets the system by asking each service to reset its own data")
                .check(CLASSES);
    }

    /** Kafka is the scan feed's transport and nothing else's: checkout produces, analytics consumes. */
    @Test
    void onlyCheckoutAndAnalyticsUseKafka() {
        noClasses().that().resideInAnyPackage(service("catalog"), service("inventory"), service("admin"))
                .should().dependOnClassesThat().resideInAPackage("org.apache.kafka..")
                .check(CLASSES);
        noClasses().that().resideInAPackage(service("checkout"))
                .should().dependOnClassesThat().resideInAPackage("org.apache.kafka.clients.consumer..")
                .because("checkout-service only publishes scan events")
                .check(CLASSES);
        noClasses().that().resideInAPackage(service("analytics"))
                .should().dependOnClassesThat().resideInAPackage("org.apache.kafka.clients.producer..")
                .because("analytics-service only consumes scan events")
                .check(CLASSES);
    }

    /** The stock-commit step of the saga is a contract between exactly two services. */
    @Test
    void stockCommitContractIsBetweenCheckoutAndInventoryOnly() {
        noClasses().that().resideInAnyPackage(service("catalog"), service("analytics"), service("admin"))
                .should().dependOnClassesThat().haveSimpleNameStartingWith("StockCommit")
                .check(CLASSES);
    }

    /** The common library is the bottom of the stack. */
    @Test
    void commonDependsOnNothingOfOurs() {
        noClasses().that().resideInAnyPackage(
                        BASE + ".contract..", BASE + ".domain..", BASE + ".web..", BASE + ".client..")
                .should().dependOnClassesThat().resideInAnyPackage(BASE + ".service..")
                .check(CLASSES);
    }

    /**
     * Status codes are chosen in exactly one place, {@code GlobalExceptionHandler}.
     * Controllers bind HTTP, and outbound clients read the statuses other
     * services return; service logic does neither.
     */
    @Test
    void serviceLogicDoesNotChooseHttpStatuses() {
        noClasses().that().resideInAPackage(BASE + ".service..")
                .and().haveSimpleNameNotEndingWith("Controller")
                .and().haveSimpleNameNotEndingWith("Client")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.http..", "jakarta.servlet..")
                .because("choosing HTTP status codes is GlobalExceptionHandler's job")
                .check(CLASSES);
    }
}
