import { docClient } from '../lib/aws-clients';
import { ProfileRepository } from './ProfileRepository';
import { RoomRepository } from './RoomRepository';
import { GroupRepository } from './GroupRepository';
import { SectionItemRepository } from './SectionItemRepository';
import { DocumentSectionRepository } from './DocumentSectionRepository';
import { DocumentCommentRepository } from './DocumentCommentRepository';
import { SectionReviewRepository } from './SectionReviewRepository';
import { DocumentLifecycleRepository } from './DocumentLifecycleRepository';
import { DocumentReviewRequestRepository } from './DocumentReviewRequestRepository';
import { VideoSessionRepository } from './VideoSessionRepository';
import { createVideoSessionStore } from './videoSessionStoreFactory';
import type { VideoSessionStore } from './VideoSessionStore';
import { DocumentTypeRepository } from './DocumentTypeRepository';
import { TypedDocumentRepository } from './TypedDocumentRepository';
import { ApprovalRepository } from './ApprovalRepository';
import { PipelineRunRepository } from './PipelineRunRepository';
import { PipelineDLQRepository } from './PipelineDLQRepository';
import { CallRecordingRepository } from './CallRecordingRepository';
import { CallReactionRepository } from './CallReactionRepository';
import { HangoutRoomRepository } from './HangoutRoomRepository';
import { HangoutRoomMemberRepository } from './HangoutRoomMemberRepository';
import { ChangeVerdictRepository } from './ChangeVerdictRepository';
import { CallLogRepository } from './CallLogRepository';

// Singleton instances — share the same docClient across all repositories
export const profileRepo = new ProfileRepository(docClient);
export const roomRepo = new RoomRepository(docClient);
export const groupRepo = new GroupRepository(docClient);
export const sectionItemRepo = new SectionItemRepository(docClient);
export const documentSectionRepo = new DocumentSectionRepository(docClient);
export const documentCommentRepo = new DocumentCommentRepository(docClient);
export const sectionReviewRepo = new SectionReviewRepository(docClient);
export const documentLifecycleRepo = new DocumentLifecycleRepository(docClient);
export const documentReviewRequestRepo = new DocumentReviewRequestRepository(docClient);
// videoSessionRepo is typed as the VideoSessionStore interface so callers
// don't bind to the DDB implementation. The factory honours the
// VIDEO_SESSIONS_STORE env flag — see videoSessionStoreFactory.ts.
export const videoSessionRepo: VideoSessionStore = createVideoSessionStore(docClient);
export const documentTypeRepo = new DocumentTypeRepository(docClient);
export const typedDocumentRepo = new TypedDocumentRepository(docClient);
export const approvalRepo = new ApprovalRepository(docClient);
export const pipelineRunRepo = new PipelineRunRepository(docClient);
export const pipelineDLQRepo = new PipelineDLQRepository(docClient);
export const callRecordingRepo = new CallRecordingRepository(docClient);
export const callReactionRepo = new CallReactionRepository(docClient);
export const hangoutRoomRepo = new HangoutRoomRepository(docClient);
export const hangoutRoomMemberRepo = new HangoutRoomMemberRepository(docClient);
export const changeVerdictRepo = new ChangeVerdictRepository(docClient);

export { BaseRepository } from './BaseRepository';
export { ProfileRepository, ProfileItem } from './ProfileRepository';
export { RoomRepository, RoomItem, RoomMemberItem } from './RoomRepository';
export { GroupRepository, GroupItem, GroupMemberItem } from './GroupRepository';
export { SectionItemRepository, SectionItemFields, CreateSectionItemInput } from './SectionItemRepository';
export { DocumentSectionRepository, DocumentSectionFields, CreateDocumentSectionInput } from './DocumentSectionRepository';
export { DocumentCommentRepository, DocumentComment } from './DocumentCommentRepository';
export { SectionReviewRepository, SectionReview } from './SectionReviewRepository';
export {
  DocumentLifecycleRepository,
  DocumentLifecycleItem,
  LifecycleHistoryEntry,
  LifecycleConcurrencyError,
  HISTORY_LIMIT,
} from './DocumentLifecycleRepository';
export {
  DocumentReviewRequestRepository,
  DocumentReviewRequest,
  CreateReviewRequestInput,
  ReviewRequestState,
  REVIEW_REQUEST_STATES,
  REVIEWER_RESPONSE_STATES,
  DOC_SCOPE,
  WITHDRAWN_RETENTION_DAYS,
  buildRequestKey,
  isReviewOutstanding,
  isOverdue,
} from './DocumentReviewRequestRepository';
export { VideoSessionRepository } from './VideoSessionRepository';
export {
  VideoSessionStore,
  VideoSessionRecord,
  VideoSessionParticipant,
  DEFAULT_VIDEO_SESSION_TTL_DAYS,
  computeEndedSessionTtl,
} from './VideoSessionStore';
export { MemoryVideoSessionStore } from './MemoryVideoSessionStore';
export {
  createVideoSessionStore,
  resolveVideoSessionStoreMode,
  VideoSessionStoreMode,
} from './videoSessionStoreFactory';
export {
  DocumentTypeRepository,
  DocumentTypeItem,
  DocumentTypeFieldItem,
  DocumentTypeFieldKind,
  DocumentTypeFieldWidget,
  DocumentTypeFieldCardinality,
  DocumentTypeFieldValidation,
  DocumentTypeFieldShowWhen,
  DocumentTypeFieldDisplayModes,
  DocumentTypeVersionSnapshot,
} from './DocumentTypeRepository';
export {
  TypedDocumentRepository,
  TypedDocumentItem,
  TypedDocumentValue,
} from './TypedDocumentRepository';
export {
  ApprovalRepository,
  ApprovalEntry,
  ApprovalDecision,
} from './ApprovalRepository';
export {
  PipelineRunRepository,
  PipelineRunItem,
} from './PipelineRunRepository';
export {
  PipelineDLQRepository,
  PipelineDLQItem,
} from './PipelineDLQRepository';
export {
  CallRecordingRepository,
  CallRecording,
  CallRecordingParticipant,
  RecordingListRow,
  RecordingSegment,
  ComposeStatus,
  RecordingMediaState,
  describeMedia,
  deriveComposeStatus,
  CALL_RECORDINGS_TABLE,
  CALL_RECORDING_PARTICIPANTS_TABLE,
  RECORDING_ID_INDEX,
} from './CallRecordingRepository';
export {
  CallReactionRepository,
  CallReaction,
  CallReactionInput,
  CALL_REACTIONS_TABLE,
} from './CallReactionRepository';
export {
  HangoutRoomRepository,
  HangoutRoom,
  HangoutRoomSettings,
  HangoutRoomState,
  HangoutRoomVisibility,
  CreateHangoutRoomInput,
  ListRoomsOpts as ListHangoutRoomsOpts,
  ListRoomsResult as ListHangoutRoomsResult,
  SearchRoomsOpts as SearchHangoutRoomsOpts,
  RoomSlugConflictError,
  RoomNotFoundError,
  RoomAccessDeniedError,
  RoomArchivedError,
  HANGOUT_ROOMS_TABLE,
  SEARCH_MAX_RESULTS as HANGOUT_ROOMS_SEARCH_MAX,
} from './HangoutRoomRepository';
export {
  HangoutRoomMemberRepository,
  HangoutRoomMember,
  HangoutRoomMemberRole,
  ListRoomsForUserOpts,
  ListRoomsForUserResult,
  HANGOUT_ROOM_MEMBERS_TABLE,
} from './HangoutRoomMemberRepository';
export {
  ChangeVerdictRepository,
  ChangeVerdictRow,
  CHANGE_VERDICTS_TABLE,
  changeKey,
  toVerdict,
} from './ChangeVerdictRepository';
export const callLogRepo = new CallLogRepository(docClient);
