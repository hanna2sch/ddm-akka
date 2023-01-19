package de.ddm.actors.profiling;

import akka.actor.Actor;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.profiling.Tuple;
import de.ddm.actors.profiling.Column;
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
import java.util.stream.Collectors;

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
	public static class ConfirmationMessage implements Message {
		private static final long serialVersionUID = 4591192342652568030L;
		Comparer c;
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
	private List<int[]> permutations = new ArrayList<>();
	//private List<Tuple> working = new ArrayList<>();
	//private List<Tuple> done = new ArrayList<>();
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	private List<Column> columns = new ArrayList<>();
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
				.onMessage(ConfirmationMessage.class, this::handle)
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
		if(message.getBatch().size() != 0){
			List<List<String>> templist = message.getBatch();
			int colnumber = 0;
			for(List<String> tempentry : templist){
				List<String> distict_tempentry = tempentry.stream().distinct().collect(Collectors.toList());
				this.columns.add(new Column(message.getId(),colnumber, distict_tempentry));
				colnumber+=1;

				for (int i = -1; i < this.columns.size(); i++){
					if(i==-1 || i==(this.columns.size()-1)) continue;
					int array[] = new int[2];
					array[0] = this.columns.size()-1;
					array[1] = i;
					this.permutations.add(array);
				}
			}
		}
		this.getContext().getLog().info("columnlistsize: {}, permutations: {}", this.columns.size(), this.permutations.size());

		//TODO: wenn batch zu groß, dann sinnvoll splitten und erst dann zu batchMessage hinzufügen
		//adding batching && batch confirmation
		//this.getContext().getLog().info("Batch added, Tuple_size: {}", this.permutations.size());
		return this;
	}
	private Behavior<Message> handle(ConfirmationMessage message) {
		//bekommt ids von den batches wieder, die verglichen wurden
		boolean check = false;
		/*for (Tuple t : this.working){
			if (t.equals(temp)){
				this.done.add(t);
				this.working.remove(t);
				break;
			}
		}
		this.getContext().getLog().info("pending: {}, working: {}, done: {}", this.permutations.size(), this.working.size(), this.done.size());*/
		return this;
	}

	private void handle_(ActorRef<DependencyWorker.Message> dependencyWorker) {
		if(!permutations.isEmpty()){
			int currenttask[] = this.permutations.get(this.permutations.size() - 1);
			this.permutations.remove(this.permutations.size()-1);
			Column c = this.columns.get(currenttask[0]);
			Column cc = this.columns.get(currenttask[1]);

			this.getContext().getLog().info("currenttask: File {},{} comparing File{},{}",c.getFileId(),c.getColId(), cc.getFileId(), cc.getColId());
			dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,c.getFileId(), c.getColId(), cc.getFileId(), cc.getColId(), c.getColData(), cc.getColData()));
		}

		/*if(!permutations.isEmpty()){
			Tuple currenttask = this.permutations.get(0);
			this.permutations.remove(0);
			this.working.add(currenttask);
			BatchMessage b=this.batchMessages.get(currenttask.getA());
			BatchMessage bb=this.batchMessages.get(currenttask.getB());
			try{
				dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, b.getId(), b.getId(), bb.getId(), b.getBatch(), bb.getBatch()));
			}catch(Exception e){
				this.working.remove(currenttask);
				this.permutations.add(currenttask);
			}
		} else if (this.working.isEmpty()) end();*/
	}


	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			if(permutations.isEmpty()) {
				dependencyWorker.tell(new DependencyWorker.WaitingMessage(this.largeMessageProxy));
			}
			this.getContext().getLog().info("Message told to Dep Worker");
			handle_(dependencyWorker);
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
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
					this.getContext().getLog().info("!!!!!!!!!!!!!!!!!!!!!!!! Dep found: File{}Col{}->File{}Col{}", com.getFileid(), com.getCompare_colid(), com.getCompare_fileid(), com.getCompare_colid());
					countResultCollector += inds.size();
					this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
				}
			}

		}
		handle_(dependencyWorker);
		if(this.permutations.isEmpty()) end();
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