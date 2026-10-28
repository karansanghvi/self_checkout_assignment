package com.school.selfcheckout.architecture;

import com.school.selfcheckout.messaging.Topic;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Machine-checked statement of the microservice boundaries.
 *
 * Maven already stops a service from compiling against another service --
 * none declares one as a dependency. This test puts all five on a single
 * classpath anyway, so the rules about <i>how</i> they may talk can be stated
 * and checked in one place. Since week 6 the answer is: only through Kafka.
 *
 * <pre>
 *   topic                   producer               consumers
 *   scan-events             checkout (outbox)      analytics
 *   transaction-completed   checkout (outbox)      inventory
 *   catalog-snapshots       catalog                checkout, inventory, analytics (via common CatalogFeed)
 *   reset-requested         admin                  catalog
 *   reset-completed         all four               admin
 * </pre>
 *
 * The only HTTP left is load client -> gateway -> the service that owns the
 * endpoint.
 *
 * Packages:
 *
 *   service.{catalog,checkout,inventory,analytics,admin}   the deployable services
 *   contract, domain, web, client, messaging               the common library
 */
class ServiceBoundariesTest {

    private static final String BASE = "com.school.selfcheckout";

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages(BASE);

    private static final List<String> SERVICES = List.of("catalog", "checkout", "inventory", "analytics", "admin");

    private static final String[] HTTP_CLIENTS = {
            "java.net.http..",
            "java.net.HttpURLConnection",
            "org.springframework.web.client..",
            "org.springframework.web.reactive.function.client..",
            "org.apache.hc.."};

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
                .because("services talk through Kafka, never through each other's classes")
                .check(CLASSES);
    }

    /** The heart of week 6: no service, and nothing in common, can make an HTTP call. */
    @Test
    void nothingMakesOutboundHttpCalls() {
        noClasses().that().resideInAPackage(BASE + "..")
                .should().dependOnClassesThat().resideInAnyPackage(HTTP_CLIENTS)
                .because("every service-to-service interaction is an event on Kafka")
                .check(CLASSES);
    }

    /** Week 5's /internal/** endpoints are gone; every remaining endpoint is public and routed by the gateway. */
    @Test
    void noServiceExposesAnInternalEndpoint() {
        List<String> internal = new ArrayList<>();
        for (JavaClass javaClass : CLASSES) {
            if (!javaClass.getPackageName().startsWith(BASE + ".service.")) {
                continue;
            }
            for (JavaMethod method : javaClass.getMethods()) {
                RequestMapping mapping = AnnotatedElementUtils.findMergedAnnotation(
                        method.reflect(), RequestMapping.class);
                if (mapping == null) {
                    continue;
                }
                for (String path : mapping.path()) {
                    if (path.startsWith("/internal")) {
                        internal.add(javaClass.getSimpleName() + "#" + method.getName() + " " + path);
                    }
                }
            }
        }
        assertThat(internal).as("internal HTTP endpoints").isEmpty();
    }

    /**
     * Which service touches which topic. {@link Topic} is an enum precisely
     * so these accesses are visible here. checkout names its outbox topics
     * when it writes outbox rows; the shared CatalogFeed (in common) names
     * catalog-snapshots on behalf of the three services that use it.
     */
    @Test
    void eachServiceTouchesOnlyItsOwnTopics() {
        Map<String, Set<String>> expected = new TreeMap<>(Map.of(
                "catalog", Set.of("RESET_REQUESTED", "CATALOG_SNAPSHOTS"),
                "checkout", Set.of("SCAN_EVENTS", "TRANSACTION_COMPLETED"),
                "inventory", Set.of("TRANSACTION_COMPLETED", "TRANSACTION_COMPLETED_DLT"),
                "analytics", Set.of("SCAN_EVENTS"),
                "admin", Set.of("RESET_REQUESTED", "RESET_COMPLETED")));

        Map<String, Set<String>> actual = new TreeMap<>();
        for (String service : SERVICES) {
            Set<String> topics = new TreeSet<>();
            for (JavaClass javaClass : CLASSES) {
                if (!javaClass.getPackageName().startsWith(BASE + ".service." + service)) {
                    continue;
                }
                for (JavaFieldAccess access : javaClass.getFieldAccessesFromSelf()) {
                    if (access.getTargetOwner().isEquivalentTo(Topic.class)) {
                        topics.add(access.getTarget().getName());
                    }
                }
            }
            actual.put(service, topics);
        }
        assertThat(actual).isEqualTo(expected);
    }

    /** admin-service resets the system by asking; it owns no data, so it has no way to reach a database. */
    @Test
    void adminHasNoDatabase() {
        noClasses().that().resideInAPackage(service("admin"))
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.jdbc..", "javax.sql..", "java.sql..")
                .because("admin-service resets the system by publishing an event")
                .check(CLASSES);
    }

    /** The common library is the bottom of the stack. */
    @Test
    void commonDependsOnNothingOfOurs() {
        noClasses().that().resideInAnyPackage(
                        BASE + ".contract..", BASE + ".domain..", BASE + ".web..", BASE + ".client..",
                        BASE + ".messaging..")
                .should().dependOnClassesThat().resideInAnyPackage(BASE + ".service..")
                .check(CLASSES);
    }

    /**
     * Status codes are chosen in exactly one place, {@code GlobalExceptionHandler}.
     * Controllers bind HTTP; service logic, and the event handlers, do not.
     */
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
