package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
//TODO: Error:  Encoder(akka://ddm)| Failed to serialize oversized message [de.ddm.actors.profiling.DependencyWorker$TaskMessage].
//akka.remote.OversizedPayloadException: Discarding oversized payload sent to Some(Actor[]):
// max allowed size 262144 bytes. Message type [de.ddm.actors.profiling.DependencyWorker$TaskMessage].
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<List<String>> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		List<Comparer> result;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		//this.getContext().getLog().info("Hello from DepMiner Constructor");
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.countResultCollector = 0;
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();
		this.batchMessages = new ArrayList<>();
		//this.getContext().getLog().info("Hello from DepMiner Constructor_end");
		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
		tasks = new ArrayList<>();
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final List<BatchMessage> batchMessages;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private int countResultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	private final List<Task> tasks;
	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		this.getContext().getLog().info("inputReadersize: {}", this.inputReaders.size());
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		//this.getContext().getLog().info("Hello from HeaderMEssage");
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}


	private Behavior<Message> handle(BatchMessage message) {
		if (message.getBatch().size() != 0)
			this.batchMessages.add(message);
		return this;
	}

	private int count = 0;
	private int count2 = 0;
	private boolean batchtoggle = false;
	private void handle(ActorRef<DependencyWorker.Message> depW){ //TODO: Dependencyworker muss hier Arbeit zugewiesen bekommen
		// tasks.add(new Task(count, count2, this.batchMessages.get(this.count).getBatch(), this.batchMessages.get(this.count2).getBatch()));
		this.getContext().getLog().info("inputreadersize: {}, count: {}, batchtoggle: {}", inputReaders.size(), count, batchtoggle);
		if(this.batchMessages.size() != this.inputReaders.size()){
			batchtoggle = true;
			depW.tell(new DependencyWorker.WaitingMessage(this.largeMessageProxy));
		}
		else if(this.count >= this.inputReaders.size() && !this.batchtoggle) this.end();
		else if (this.count >= this.batchMessages.size()&& this.batchtoggle) return;
		else {
			tasks.add(new Task(count, count2, this.batchMessages.get(this.count).getBatch(), this.batchMessages.get(this.count2).getBatch()));
			BatchMessage b = this.batchMessages.get(this.count);
			BatchMessage bb = this.batchMessages.get(this.count2);
			this.count2 += 1;
			if (this.count2 >= this.batchMessages.size()) {
				this.count += 1;
				this.count2 = this.count;
			}
			this.batchtoggle =true;
			//TODO: implement following with task-organizer
			depW.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, b.getId(), b.getId(), bb.getId(), b.getBatch(), bb.getBatch()));
		}
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		this.getContext().getLog().info("Hello from RegistrationMessage");
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
			if(this.batchMessages.size() != this.inputReaders.size()) {
				batchtoggle =true;
				//this.getContext().getLog().info("!!!!!!!!!!new Batchcount: {}", batchtoggle);
				dependencyWorker.tell(new DependencyWorker.WaitingMessage(this.largeMessageProxy));
			}
			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
			this.getContext().getLog().info("Message told to Dep Worker");
			handle(dependencyWorker);
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		//this.getContext().getLog().info("Hello from CompletionMessage");
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.
		this.batchtoggle =false;
		//this.getContext().getLog().info("!!!!!!!!!!new -Batchcount: {}", batchtoggle);
		if (this.headerLines[0] != null && !(message.getResult().isEmpty())) {

			for (Comparer com : message.getResult()){
				if(com.getFileid()!=com.getCompare_fileid() && com.getColid()!=com.getCompare_colid()){
					int dependent = com.getFileid();
					int referenced = com.getCompare_fileid();
					File dependentFile = this.inputFiles[dependent];
					File referencedFile = this.inputFiles[referenced];
					String[] dependentAttributes = {this.headerLines[dependent][com.getColid()]};
					String[] referencedAttributes = {this.headerLines[referenced][com.getCompare_colid()]};
					List<InclusionDependency> inds = new ArrayList<>(1);
					InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
					inds.add(ind);
					//this.getContext().getLog().info("!!!!!!!!!!!!!!!!!!!!!!!! Dep found: File{}Col{}->File{}Col{}", com.getFileid(), com.getCompare_colid(), com.getCompare_fileid(), com.getCompare_colid());
					countResultCollector += inds.size();
					this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
				}
			}

		}
		handle(dependencyWorker);
		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
		//
		//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (this.count >= this.inputReaders.size() && !this.batchtoggle) this.end();
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
		this.getContext().getLog().info("Found {} INDs!", this.countResultCollector);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}