package com.school.selfcheckout;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Machine-checked statement of the layered architecture.
 *
 * The package layout alone is only a convention; these rules are what stop it
 * from eroding. Each one corresponds to a specific way the layering was blurred
 * before the refactor, so a regression fails the build rather than passing
 * review.
 *
 * Layers, top to bottom:
 *
 *   api                       HTTP: routing, status codes, error bodies
 *   transactions / analytics  business logic (admin sits alongside them)
 *   catalog                   shared read model, used by both
 *   repository                database access
 *
 * plus three dependency-free leaves any layer may use: {@code contract} (wire
 * DTOs), {@code domain} (business records and errors), and {@code config}.
 */
class LayeringTest {

    private static final JavaClasses PRODUCTION_CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("com.school.selfcheckout");

    /** Guards against the rules below silently passing on an empty import. */
    @Test
    void theProductionClassesWereActuallyImported() {
        assertThat(PRODUCTION_CLASSES).hasSizeGreaterThan(20);
    }

    @Test
    void layersAreRespected() {
        layeredArchitecture()
                .consideringOnlyDependenciesInAnyPackage("com.school.selfcheckout..")

                .layer("API").definedBy("..api..")
                .layer("Transactions").definedBy("..transactions..")
                .layer("Analytics").definedBy("..analytics..")
                .layer("Admin").definedBy("..admin..")
                .layer("Catalog").definedBy("..catalog..")
                .layer("Persistence").definedBy("..repository..")

                // Nothing may reach up into the HTTP layer.
                .whereLayer("API").mayNotBeAccessedByAnyLayer()

                .whereLayer("Transactions").mayOnlyBeAccessedByLayers("API")
                .whereLayer("Admin").mayOnlyBeAccessedByLayers("API")

                // Checkout records scans for popularity, and a reset clears the
                // in-memory window -- both documented inbound calls to analytics.
                .whereLayer("Analytics").mayOnlyBeAccessedByLayers("API", "Transactions", "Admin")

                .whereLayer("Catalog")
                .mayOnlyBeAccessedByLayers("API", "Transactions", "Analytics", "Admin")

                // The database layer is reachable from the business layers only.
                .whereLayer("Persistence")
                .mayOnlyBeAccessedByLayers("Transactions", "Analytics", "Admin", "Catalog")

                .check(PRODUCTION_CLASSES);
    }

    /**
     * The violation this refactor was mostly about: services used to import
     * {@code api.dto.Dtos} and {@code api.ApiException}, inverting the arrow.
     */
    @Test
    void nothingBelowTheApiLayerDependsOnIt() {
        noClasses().that().resideOutsideOfPackage("..api..")
                .should().dependOnClassesThat().resideInAPackage("..api..")
                .because("the API layer is the top of the stack; wire DTOs live in ..contract..")
                .check(PRODUCTION_CLASSES);
    }

    /** Controllers must go through a service, never straight to SQL. */
    @Test
    void controllersDoNotReachTheDatabase() {
        noClasses().that().resideInAPackage("..api..")
                .should().dependOnClassesThat().resideInAPackage("..repository..")
                .because("the API layer must not skip the business layers")
                .check(PRODUCTION_CLASSES);
    }

    /** The database layer is a leaf: it knows domain records and nothing else of ours. */
    @Test
    void repositoriesDoNotCallUpwards() {
        noClasses().that().resideInAPackage("..repository..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "..api..", "..transactions..", "..analytics..", "..catalog..", "..admin..")
                .because("business logic belongs in a service, not in a repository")
                .check(PRODUCTION_CLASSES);
    }

    /**
     * Keeps HTTP out of the lower layers. This is what replacing
     * {@code ApiException} with {@code CheckoutException} bought: status codes
     * are now chosen in exactly one place, {@code GlobalExceptionHandler}.
     */
    @Test
    void lowerLayersDoNotKnowAboutHttp() {
        noClasses().that().resideInAnyPackage(
                        "..transactions..", "..analytics..", "..catalog..",
                        "..admin..", "..repository..", "..domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.web..", "org.springframework.http..", "jakarta.servlet..")
                .because("choosing HTTP status codes is the API layer's job")
                .check(PRODUCTION_CLASSES);
    }

    /** Layers must form a DAG -- no two packages may depend on each other. */
    @Test
    void layersAreFreeOfCycles() {
        slices().matching("com.school.selfcheckout.(*)..")
                .should().beFreeOfCycles()
                .check(PRODUCTION_CLASSES);
    }
}
