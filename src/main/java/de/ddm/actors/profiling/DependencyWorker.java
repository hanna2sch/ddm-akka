package de.ddm.actors.profiling;

import akka.actor.Actor;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int f_id;
		int c_id;
		int ff_id;
		int bb_id;
		List<String> b;
		List<String> bb;
	}
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WaitingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216252L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
	}
	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(WaitingMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		this.getContext().getLog().info("Worker received Message!");
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		List<String> b = message.getB();
		List<String> bb = message.getBb();
		List<Comparer> result = new ArrayList<>();
		if(b.containsAll(bb)) {
			Comparer c_temp = new Comparer(message.getF_id(),message.getFf_id(),message.getC_id(), message.getBb_id());
			result.add(c_temp);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new DependencyMiner.ConfirmationMessage(c_temp), message.getDependencyMinerLargeMessageProxy()));
		} else if (bb.containsAll(b)) {
			Comparer c_temp = new Comparer(message.getFf_id(),message.getF_id(),message.getBb_id(), message.getC_id());
			result.add(c_temp);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new DependencyMiner.ConfirmationMessage(c_temp), message.getDependencyMinerLargeMessageProxy()));
		}
		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private Behavior<Message> handle(WaitingMessage message) throws InterruptedException {
		this.getContext().getLog().info("Sleeping!");
		Thread.sleep(2345);
		LargeMessageProxy.LargeMessage temp_message = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), new ArrayList<>());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(temp_message, message.getDependencyMinerLargeMessageProxy()));
		return this;
	}
}
