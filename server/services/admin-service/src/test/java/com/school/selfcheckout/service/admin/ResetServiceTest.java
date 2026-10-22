package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.domain.ErrorCode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.web.client.ResourceAccessException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The reset is a protocol across four services; these pin its order, its command, and its failure mode. */
class ResetServiceTest {

    private static final List<ResetService.Target> TARGETS = List.of(
            new ResetService.Target("catalog-service", "http://catalog"),
            new ResetService.Target("inventory-service", "http://inventory"),
            new ResetService.Target("analytics-service", "http://analytics"),
            new ResetService.Target("checkout-service", "http://checkout"));

    private final ResetClient client = mock(ResetClient.class);

    private ResetService newService(int catalogSize, int stockPerItem) {
        when(client.reset(anyString(), any())).thenAnswer(inv ->
                new ResetAck(inv.getArgument(0), ((ResetCommand) inv.getArgument(1)).epoch(), 1));
        return new ResetService(client, TARGETS, catalogSize, stockPerItem);
    }

    @Test
    void resetsCatalogFirstAndAnalyticsBeforeCheckout() {
        newService(2000, 10000).reset();

        InOrder order = inOrder(client);
        order.verify(client).reset(eq("http://catalog"), any());
        order.verify(client).reset(eq("http://inventory"), any());
        order.verify(client).reset(eq("http://analytics"), any());
        order.verify(client).reset(eq("http://checkout"), any());
    }

    @Test
    void everyServiceGetsTheSameCommand() {
        newService(1234, 99).reset(null, null);

        ArgumentCaptor<ResetCommand> commands = ArgumentCaptor.forClass(ResetCommand.class);
        verify(client, org.mockito.Mockito.times(4)).reset(anyString(), commands.capture());
        assertThat(commands.getAllValues()).hasSize(4).allMatch(c -> c.equals(commands.getValue()));
        assertThat(commands.getValue().catalogSize()).isEqualTo(1234);
        assertThat(commands.getValue().stockPerItem()).isEqualTo(99);
    }

    @Test
    void explicitOverridesWinAndResolveIndependently() {
        newService(1234, 99).reset(7, null);

        ArgumentCaptor<ResetCommand> command = ArgumentCaptor.forClass(ResetCommand.class);
        verify(client).reset(eq("http://catalog"), command.capture());
        assertThat(command.getValue().catalogSize()).isEqualTo(7);
        assertThat(command.getValue().stockPerItem()).isEqualTo(99);
    }

    @Test
    void epochsIncreaseFromOneResetToTheNext() throws Exception {
        ResetService service = newService(2000, 10000);
        long first = (long) service.reset().get("epoch");
        Thread.sleep(2);
        long second = (long) service.reset().get("epoch");

        assertThat(second).isGreaterThan(first);
    }

    @Test
    void stopsAtTheFirstFailureAndNamesTheService() {
        ResetService service = newService(2000, 10000);
        when(client.reset(eq("http://inventory"), any())).thenThrow(new ResourceAccessException("Connection refused"));

        assertThatThrownBy(service::reset)
                .isInstanceOf(CheckoutException.class)
                .hasMessageContaining("inventory-service")
                .extracting(e -> ((CheckoutException) e).getErrorCode())
                .isEqualTo(ErrorCode.DEPENDENCY_FAILED);
        verify(client, never()).reset(eq("http://analytics"), any());
        verify(client, never()).reset(eq("http://checkout"), any());
    }

    @Test
    void reportsEveryServicesAck() {
        Map<String, Object> result = newService(2000, 10000).reset();

        assertThat(result).containsEntry("status", "ok")
                .containsEntry("catalogSize", 2000)
                .containsEntry("stockPerItem", 10000);
        assertThat((List<?>) result.get("services")).hasSize(4);
    }
}
