from enum import Enum, auto
from app import logger


class TaskType(Enum):
    STATES_MEAN = auto()
    STATE_MEAN = auto()
    BEST_5 = auto()
    WORST_5 = auto()
    GLOBAL_MEAN = auto()
    DIFF_FROM_MEAN = auto()
    STATE_DIFF_FROM_MEAN = auto()
    MEAN_BY_CATEGORY = auto()
    STATE_MEAN_BY_CATEGORY = auto()


class Task:
    def __init__(self, task_type, di, task_param):
        self.task_id = 0
        self.task_type = task_type
        self.di = di
        self.task_param = task_param
        self.is_done = False

    def execute_task(self):
        if self.task_type == TaskType.STATES_MEAN:
            res = self.execute_states_mean()
        if self.task_type == TaskType.STATE_MEAN:
            res = self.execute_state_mean()
        if self.task_type == TaskType.BEST_5:
            res = self.execute_best_5()
        if self.task_type == TaskType.WORST_5:
            res = self.execute_worst_5()
        if self.task_type == TaskType.GLOBAL_MEAN:
            res = self.execute_global_mean()
        if self.task_type == TaskType.DIFF_FROM_MEAN:
            res = self.execute_diff_from_mean()
        if self.task_type == TaskType.STATE_DIFF_FROM_MEAN:
            res = self.execute_state_diff_from_mean()
        if self.task_type == TaskType.MEAN_BY_CATEGORY:
            res = self.execute_mean_by_category()
        if self.task_type == TaskType.STATE_MEAN_BY_CATEGORY:
            res = self.execute_state_mean_by_category()

        self.write_to_file(res)

    def write_to_file(self, res):
        logger.info("starting write into file task id %d", self.task_id)
        filename_path = f"./results/{self.task_id}"
        with open(filename_path, "w") as f:
            f.write(str(res))
        self.is_done = True
        logger.info("Write in file Done")

    def execute_states_mean(self):
        logger.info("execute states_mean task %d", self.task_id)
        mean_states = self.calculate_states_mean_by_question()
        json_res = dict(sorted(mean_states.items(), key=lambda item: item[1]))
        logger.info("states_mean task %d done", self.task_id)
        return json_res

    def execute_state_mean(self):
        logger.info("execute state_mean task %d", self.task_id)
        state = self.task_param.get("state")
        mean_states = self.calculate_states_mean_by_question()
        mean_res = mean_states.get(state)
        json_res = {state: float(mean_res)}
        logger.info("state_mean task %d done", self.task_id)
        return json_res

    def execute_best_5(self):
        logger.info("execute best_5 task %d", self.task_id)
        question = self.task_param.get("question")
        mean_states = self.calculate_states_mean_by_question()
        sorted_res = dict(sorted(mean_states.items(), key=lambda item: item[1]))

        if question in self.di.questions_best_is_min:
            json_res = dict(list(sorted_res.items())[:5])
        else:
            json_res = dict(list(sorted_res.items())[-5:])

        logger.info("best_5 task %d done", self.task_id)
        return json_res

    def execute_worst_5(self):
        logger.info("execute worst_5 task %d", self.task_id)
        question = self.task_param.get("question")
        mean_states = self.calculate_states_mean_by_question()
        sorted_res = dict(sorted(mean_states.items(), key=lambda item: item[1]))

        if question in self.di.questions_best_is_min:
            json_res = dict(list(sorted_res.items())[-5:])
        else:
            json_res = dict(list(sorted_res.items())[:5])

        logger.info("worst_5 task %d done", self.task_id)
        return json_res

    def execute_global_mean(self):
        global_mean = self.calculate_global_mean()
        json_res = {'global_mean': float(global_mean)}
        logger.info("global_mean task %d done", self.task_id)
        return json_res

    def execute_diff_from_mean(self):
        logger.info("execute diff_from_mean task %d", self.task_id)
        mean_states = self.calculate_states_mean_by_question()
        global_mean = self.calculate_global_mean()
        diff_from_mean = {key: float(global_mean - value) for key, value in mean_states.items()}
        json_res = dict(sorted(diff_from_mean.items(), key=lambda item: item[1]))
        logger.info("diff_from_mean task %d done", self.task_id)
        return json_res

    def execute_state_diff_from_mean(self):
        logger.info("execute state_diff_from_mean task %d", self.task_id)
        state = self.task_param.get("state")
        mean_states = self.calculate_states_mean_by_question()
        global_mean = self.calculate_global_mean()
        diff_from_mean_state = global_mean - mean_states[state]
        json_res = {state : float(diff_from_mean_state)}
        logger.info("state_diff_from_mean task %d done", self.task_id)
        return json_res

    def execute_mean_by_category(self):
        logger.info("execute mean_by_category task %d", self.task_id)
        question = self.task_param.get("question")
        df = self.di.df
        df_filtered_by_question = df[df['Question'] == question]
        grouped_mean = (df_filtered_by_question.groupby(['LocationDesc',
                                                        'StratificationCategory1',
                                                        'Stratification1'])['Data_Value']
                        .mean().reset_index())
        result_dict = {}
        for _, row in grouped_mean.iterrows():
            key = ("('" + str(row['LocationDesc']) + "', '"
                   + str(row['StratificationCategory1']) + "', '"
                   + str(row['Stratification1'] + "')"))
            result_dict[key] = row['Data_Value']

        logger.info("mean_by_category task %d done", self.task_id)
        return result_dict

    def execute_state_mean_by_category(self):
        logger.info("execute state_mean_by_category task %d", self.task_id)
        question = self.task_param.get("question")
        state = self.task_param.get("state")
        df = self.di.df
        df_filtered_by_question = df[(df['Question'] == question) & (df['LocationDesc'] == state)]
        grouped_mean = df_filtered_by_question.groupby(['StratificationCategory1',
                                                        'Stratification1'])[
            'Data_Value'].mean().reset_index()
        result_dict = {}
        for _, row in grouped_mean.iterrows():
            key = ("('" + str(row['StratificationCategory1']) + "', '"
                   + str(row['Stratification1'] + "')"))
            result_dict[key] = row['Data_Value']

        state_tes_dict = {state : result_dict}
        logger.info("state_mean_by_category task %d done", self.task_id)
        return state_tes_dict

    def calculate_global_mean(self):
        question = self.task_param.get("question")
        df = self.di.df
        df_filtered_by_question = df[df['Question'] == question]
        global_mean = df_filtered_by_question['Data_Value'].mean()
        return global_mean

    def calculate_states_mean_by_question(self):
        question = self.task_param.get("question")
        df = self.di.df
        df_filtered_by_question = df[df['Question'] == question]
        dict_res = df_filtered_by_question.groupby('LocationDesc')['Data_Value'].mean()
        return dict_res
